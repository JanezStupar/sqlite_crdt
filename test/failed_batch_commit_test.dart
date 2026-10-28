import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqflite_common_ffi/sqflite_ffi.dart' show databaseFactoryFfi;
import 'package:sqlite_crdt/sqlite_crdt.dart';
import 'package:sqlite_crdt/src/batch_executor.dart';
import 'package:sqlite_crdt/src/sqlite_api.dart';
import 'package:test/test.dart';

// An independent read-only connection holds a real SQLite shared lock. No
// backend exceptions or transaction state are mocked.
const _readerScript = r"""
import json, sqlite3, sys
connection = sqlite3.connect('file:' + sys.argv[1] + '?mode=ro', uri=True,
                            isolation_level=None, timeout=0.1)
connection.row_factory = sqlite3.Row
connection.execute('PRAGMA query_only=ON')
connection.execute('BEGIN')
connection.execute('SELECT * FROM items').fetchall()
print('ready', flush=True)
for command in sys.stdin:
    if command.strip() == 'release':
        connection.execute('ROLLBACK')
        print('released', flush=True)
    elif command.strip() == 'read':
        connection.execute('PRAGMA data_version').fetchone()
        print(json.dumps([dict(row) for row in connection.execute(
            'SELECT * FROM items ORDER BY id')]), flush=True)
connection.close()
""";

class _Fixture {
  final Directory directory;
  final SqliteCrdt db;
  _Fixture(this.directory, this.db);
  String get path => '${directory.path}/synthetic.sqlite';
  Future<List<Map<String, Object?>>> rows() =>
      db.query('SELECT * FROM items ORDER BY id');
}

Future<_Fixture> _fixture() async {
  final directory = await Directory.systemTemp.createTemp('batch-commit-');
  final db = await SqliteCrdt.open(
    '${directory.path}/synthetic.sqlite',
    singleInstance: false,
  );
  addTearDown(() async {
    await db.close();
    await directory.delete(recursive: true);
  });
  await db.execute('PRAGMA journal_mode=DELETE');
  await db.execute('PRAGMA busy_timeout=50');
  await db.execute(
    'CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT NOT NULL)',
  );
  await db.execute("INSERT INTO items(id,value) VALUES('edited','before')");
  await db.execute(
    "INSERT INTO items(id,value) VALUES('protected','unchanged')",
  );
  return _Fixture(directory, db);
}

void main() {
  for (final rollback in [false, true]) {
    test(
      'transaction merge ${rollback ? 'rolls back' : 'commits'} with local writes',
      () async {
        final fixture = await _fixture();
        final before = await fixture.rows();
        final canonicalBefore = fixture.db.canonicalTime;
        final changed = Map<String, Object?>.from(before.first);
        changed['value'] = 'remote';
        changed['hlc'] = Hlc.parse(
          changed['hlc']! as String,
        ).apply(nodeId: 'synthetic-remote').increment();
        final expectedError = StateError('abort after merge');
        final operation = fixture.db
            .transaction((transaction) async {
              expect(transaction, isA<MergeCrdtExecutor>());
              await transaction.execute(
                "UPDATE items SET value='local' WHERE id='protected'",
              );
              await (transaction as MergeCrdtExecutor).mergeIncoming(
                fixture.db,
                {
                  'items': [changed],
                },
              );
              final inside = await transaction.query(
                'SELECT * FROM items ORDER BY id',
              );
              expect(inside.map((row) => row['value']), ['remote', 'local']);
              if (rollback) throw expectedError;
            })
            .timeout(const Duration(seconds: 3));
        if (rollback) {
          await expectLater(operation, throwsA(same(expectedError)));
          expect(await fixture.rows(), before);
          expect(fixture.db.canonicalTime, canonicalBefore);
        } else {
          await operation;
          final after = await fixture.rows();
          expect(after.map((row) => row['value']), ['remote', 'local']);
          expect(after.first['hlc'], changed['hlc'].toString());
        }
      },
    );
  }

  for (final route in ['merge', 'batch', 'transaction']) {
    test(
      '$route COMMIT BUSY rolls back and releases queries and locks',
      () async {
        final fixture = await _fixture();
        final before = await fixture.rows();
        final reader = await Process.start('python3', [
          '-u',
          '-c',
          _readerScript,
          fixture.path,
        ]);
        final lines = StreamIterator(
          reader.stdout.transform(utf8.decoder).transform(const LineSplitter()),
        );
        addTearDown(() async {
          await reader.stdin.close();
          await reader.exitCode.timeout(const Duration(seconds: 5));
          await lines.cancel();
        });
        Future<String> response() async {
          expect(
            await lines.moveNext().timeout(const Duration(seconds: 5)),
            isTrue,
          );
          return lines.current;
        }

        expect(await response(), 'ready');
        Object? commitError;
        try {
          if (route == 'merge') {
            final changed = Map<String, Object?>.from(before.first);
            changed['value'] = 'after';
            changed['hlc'] = Hlc.parse(
              changed['hlc']! as String,
            ).apply(nodeId: 'synthetic-remote').increment();
            await fixture.db.merge({
              'items': [changed],
            });
          } else if (route == 'batch') {
            final batch = fixture.db.batch();
            await batch.execute(
              "UPDATE items SET value='after' WHERE id='edited'",
            );
            await batch.commit();
          } else {
            await fixture.db.transaction((transaction) async {
              await transaction.execute(
                "UPDATE items SET value='after' WHERE id='edited'",
              );
            });
          }
        } catch (error) {
          commitError = error;
        } finally {
          reader.stdin.writeln('release');
          expect(await response(), 'released');
        }
        expect(commitError, isA<DatabaseException>());
        expect((commitError as DatabaseException).getResultCode(), 5);
        expect(commitError.toString(), contains('COMMIT'));
        expect(
          await fixture.rows().timeout(const Duration(seconds: 2)),
          before,
        );
        reader.stdin.writeln('read');
        expect(jsonDecode(await response()), before);
        // A later owner can write and commit after terminal cleanup.
        await fixture.db.execute(
          "UPDATE items SET value='later' WHERE id='edited'",
        );
        expect((await fixture.rows()).first['value'], 'later');
      },
    );
  }

  test(
    'successful batch preserves results and shared CRDT timestamp',
    () async {
      final fixture = await _fixture();
      final batch = fixture.db.batch();
      await batch.execute("UPDATE items SET value='after' WHERE id='edited'");
      await batch.execute("UPDATE items SET value='also' WHERE id='protected'");
      expect(await batch.commit(), hasLength(2));
      final rows = await fixture.rows();
      expect(rows.map((row) => row['value']), ['after', 'also']);
      expect(rows.first['hlc'], rows.last['hlc']);
    },
  );

  test('batch statement failure rolls back earlier statements', () async {
    final fixture = await _fixture();
    final before = await fixture.rows();
    final batch = fixture.db.batch();
    await batch.execute("UPDATE items SET value='after' WHERE id='edited'");
    await batch.execute(
      "INSERT INTO items(id,value) VALUES('edited','duplicate')",
    );
    await expectLater(batch.commit(), throwsA(isA<DatabaseException>()));
    expect(await fixture.rows(), before);
  });

  for (final asynchronous in [false, true]) {
    test(
      'executeBatch ${asynchronous ? 'async' : 'sync'} callback failure does not commit',
      () async {
        final directory = await Directory.systemTemp.createTemp(
          'batch-callback-',
        );
        final db = await databaseFactoryFfi.openDatabase(
          '${directory.path}/synthetic.sqlite',
          options: OpenDatabaseOptions(singleInstance: false),
        );
        addTearDown(() async {
          await db.close();
          await directory.delete(recursive: true);
        });
        await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY)');
        final expected = StateError('callback failed');
        final api = ExecutorApi(db);
        await expectLater(
          api.executeBatch((batch) {
            batch.execute('INSERT INTO items VALUES(1)');
            if (asynchronous) {
              return Future<void>.delayed(Duration.zero, () => throw expected);
            }
            throw expected;
          }),
          throwsA(same(expected)),
        );
        expect(await db.rawQuery('SELECT * FROM items'), isEmpty);
        await api.executeBatch((batch) async {
          await Future<void>.delayed(Duration.zero);
          batch.execute('INSERT INTO items VALUES(2)');
        });
        expect(await db.rawQuery('SELECT * FROM items'), [
          {'id': 2},
        ]);
      },
    );
  }

  test('transaction-bound batches leave rollback to their owner', () async {
    final directory = await Directory.systemTemp.createTemp('batch-owner-');
    final db = await databaseFactoryFfi.openDatabase(
      '${directory.path}/synthetic.sqlite',
      options: OpenDatabaseOptions(singleInstance: false),
    );
    addTearDown(() async {
      await db.close();
      await directory.delete(recursive: true);
    });
    await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY)');
    final outerError = StateError('outer rollback');
    await expectLater(
      db.transaction((transaction) async {
        await transaction.execute('INSERT INTO items VALUES(1)');
        await expectLater(
          ExecutorApi(transaction).executeBatch((batch) {
            batch.execute('INSERT INTO items VALUES(1)');
          }),
          throwsA(isA<DatabaseException>()),
        );
        expect(await transaction.rawQuery('SELECT * FROM items'), [
          {'id': 1},
        ]);
        final direct = BatchExecutor(
          transaction.batch(),
          Hlc.parse('2026-10-28T12:00:00.000Z-0000-test'),
          tableFilter: (table, {schema}) => false,
        );
        await direct.execute('INSERT INTO items VALUES(2)');
        await direct.commit();
        expect(await transaction.rawQuery('SELECT * FROM items'), hasLength(2));
        throw outerError;
      }),
      throwsA(same(outerError)),
    );
    expect(await db.rawQuery('SELECT * FROM items'), isEmpty);
  });
}
