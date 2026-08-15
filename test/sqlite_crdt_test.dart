import 'dart:async';

import 'package:sqflite_common_ffi/sqflite_ffi.dart' show databaseFactoryFfi;
import 'package:sqlite_crdt/sqlite_crdt.dart';
import 'package:sqlite_crdt/src/is_web_locator.dart';
import 'package:test/test.dart';

import 'sql_crdt_test.dart';

Future<void> main() async {
  final initialDatabasesPath = await databaseFactoryFfi.getDatabasesPath();
  final crdt = await SqliteCrdt.openInMemory();

  runSqlCrdtTests(crdt);

  group('Sqlite features', () {
    setUp(() async {
      await crdt.execute('''
        CREATE TABLE users (
          id INTEGER NOT NULL,
          name TEXT,
          PRIMARY KEY (id)
        )
      ''');
    });

    tearDown(() async {
      await crdt.execute('DROP TABLE users');
    });

    test('Replace', () async {
      await insertUser(crdt, 1, 'John Doe');
      final insertHlc =
          (await crdt.query('SELECT hlc FROM users')).first['hlc'] as String;
      await crdt.execute('REPLACE INTO users (id, name) VALUES (?1, ?2)', [
        1,
        'Jane Doe',
      ]);
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'Jane Doe');
      expect((result.first['hlc'] as String).compareTo(insertHlc), 1);
    });

    test('Replace without arguments', () async {
      await insertUser(crdt, 1, 'John Doe');
      final insertHlc =
          (await crdt.query('SELECT hlc FROM users')).first['hlc'] as String;
      await crdt.execute(
        "REPLACE INTO users (id, name) VALUES (1, 'Jane Doe')",
      );
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'Jane Doe');
      expect((result.first['hlc'] as String).compareTo(insertHlc), 1);
    });

    test('Batch insert', () async {
      final batch = crdt.batch();
      insertUser(batch, 1, 'John Doe 1');
      insertUser(batch, 2, 'John Doe 2');
      await batch.commit();

      final result = await crdt.query('SELECT * FROM users');
      expect(result.length, 2);
      expect(result.first['name'], 'John Doe 1');
      expect(result.last['name'], 'John Doe 2');
      expect(result.first['hlc'], result.last['hlc']);
    });

    test('Uncommitted batch', () async {
      final batch = crdt.batch();
      insertUser(batch, 1, 'John Doe 1');
      insertUser(batch, 2, 'John Doe 2');

      final result = await crdt.query('SELECT * FROM users');
      expect(result.length, 0);
    });
  });

  test('failed commit rolls back and leaves the database usable', () async {
    final database = await SqliteCrdt.openInMemory(singleInstance: false);
    addTearDown(database.close);

    await database.execute('PRAGMA foreign_keys = ON');
    await database.execute('CREATE TABLE x (foo INTEGER PRIMARY KEY)');
    await database.execute(
      'CREATE TABLE y (foo INTEGER PRIMARY KEY '
      'REFERENCES x (foo) DEFERRABLE INITIALLY DEFERRED)',
    );

    await expectLater(
      database.transaction((transaction) async {
        await transaction.execute('INSERT INTO y (foo) VALUES (2)');
      }),
      throwsA(isA<DatabaseException>()),
    );

    expect(await database.query('SELECT * FROM y'), isEmpty);
  });

  test('transaction body errors are preserved after rollback', () async {
    final database = await SqliteCrdt.openInMemory(singleInstance: false);
    addTearDown(database.close);
    await database.execute('CREATE TABLE items (id INTEGER PRIMARY KEY)');

    final expectedError = StateError('transaction failed');
    await expectLater(
      database.transaction((transaction) async {
        await transaction.execute('INSERT INTO items (id) VALUES (1)');
        throw expectedError;
      }),
      throwsA(same(expectedError)),
    );

    expect(await database.query('SELECT * FROM items'), isEmpty);
  });

  test('excluded table writes bypass CRDT metadata', () async {
    final database = await SqliteCrdt.openInMemory(
      singleInstance: false,
      excludeTables: const {'handshake_nodes'},
    );
    addTearDown(database.close);
    final initialCanonicalTime = database.canonicalTime;

    await database.execute(
      'CREATE TABLE handshake_nodes ('
      'id INTEGER PRIMARY KEY, is_deleted INTEGER NOT NULL)',
    );
    await database.execute(
      'INSERT INTO handshake_nodes (id, is_deleted) VALUES (?1, ?2)',
      [1, 0],
    );
    expect(database.canonicalTime, initialCanonicalTime);

    final batch = database.batch();
    await batch.execute(
      'INSERT INTO handshake_nodes (id, is_deleted) VALUES (?1, ?2)',
      [2, 1],
    );
    await batch.commit();

    await database.transaction((transaction) async {
      await transaction.execute('DELETE FROM handshake_nodes WHERE id = ?1', [
        1,
      ]);
    });

    final columns = await database.query(
      'SELECT name FROM pragma_table_info(?1)',
      ['handshake_nodes'],
    );
    expect(
      columns.map((column) => column['name']),
      equals(['id', 'is_deleted']),
    );
    expect(
      await database.query('SELECT * FROM handshake_nodes'),
      equals([
        {'id': 2, 'is_deleted': 1},
      ]),
    );
  });

  test('INSERT SELECT preserves an explicit is_deleted value once', () async {
    final database = await SqliteCrdt.openInMemory(singleInstance: false);
    addTearDown(database.close);

    await database.execute(
      'CREATE TABLE source_items (id INTEGER PRIMARY KEY)',
    );
    await database.execute(
      'CREATE TABLE target_items (id INTEGER PRIMARY KEY)',
    );
    await database.execute(
      'INSERT INTO source_items (id, is_deleted) VALUES (?1, ?2)',
      [1, 1],
    );

    await database.execute(
      'INSERT INTO target_items (id, is_deleted) '
      'SELECT id, is_deleted FROM source_items',
    );

    final rows = await database.query(
      'SELECT id, is_deleted FROM target_items',
    );
    expect(
      rows,
      equals([
        {'id': 1, 'is_deleted': 1},
      ]),
    );
  });

  test('opening a database does not change the factory database path', () {
    expect(
      databaseFactoryFfi.getDatabasesPath(),
      completion(initialDatabasesPath),
    );
  });

  test('preserves the SQLite in-memory path', () {
    expect(sqliteCrdtResolvePath(inMemoryDatabasePath), inMemoryDatabasePath);
  });

  test('preserves SQLite file URIs', () {
    const fileUri = 'file:shared?mode=memory&cache=shared';
    expect(sqliteCrdtResolvePath(fileUri), fileUri);
  });
}
