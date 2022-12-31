import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqflite_common/sqlite_api.dart';

// ignore: implementation_imports
import 'package:sqflite_common/src/open_options.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:sqlparser/sqlparser.dart';
import 'package:sqlparser/utils/node_to_text.dart';
import 'package:uuid/uuid.dart';

import 'hlc.dart';

final _sqlEngine = SqlEngine();

class SqliteCrdt {
  final Database _db;
  Hlc canonicalTime;

  final _watches = <StreamController<List<Map<String, dynamic>>>, _Query>{};

  String get nodeId => canonicalTime.nodeId;

  /// Returns the last modified timestamp from other peers
  Future<Hlc?> get peerLastModified => lastModified(excludeNodeId: nodeId);

  /// Returns the last modified timestamp, optionally filtering for or against a
  /// specific node id.
  /// Useful to get "modified since" timestamps for synchronization.
  Future<Hlc?> lastModified({String? onlyNodeId, String? excludeNodeId}) =>
      _lastModified(_db, onlyNodeId: onlyNodeId, excludeNodeId: excludeNodeId);

  static Future<Iterable<String>> _getTables(Database db) async =>
      (await db.rawQuery('''
    SELECT name FROM sqlite_schema
    WHERE type ='table' AND name NOT LIKE 'sqlite_%'
  ''')).map((e) => e['name'] as String);

  static Future<Hlc?> _lastModified(Database db,
      {String? onlyNodeId, String? excludeNodeId}) async {
    assert(onlyNodeId == null || excludeNodeId == null);

    final tables = await _getTables(db);
    if (tables.isEmpty) return null;

    final whereStatement = onlyNodeId != null
        ? "WHERE hlc LIKE '%' || ?1"
        : excludeNodeId != null
            ? "WHERE hlc NOT LIKE '%' || ?1"
            : '';
    final tableStatements = tables.map((table) =>
        'SELECT max(modified) AS modified FROM $table $whereStatement');
    final result = await db.rawQuery(
      '''
      SELECT max(modified) AS modified FROM (
        ${tableStatements.join('\nUNION ALL\n')}
      )
    ''',
      [
        if (onlyNodeId != null) onlyNodeId,
        if (excludeNodeId != null) excludeNodeId,
      ],
    );
    return (result.first['modified'] as String?)?.toHlc;
  }

  SqliteCrdt._(this._db, this.canonicalTime);

  static Future<SqliteCrdt> open(
    String basePath,
    String name, {
    bool inMemory = false,
    int? version,
    FutureOr<void> Function(SqliteCrdt crdt, int version)? onCreate,
    FutureOr<void> Function(SqliteCrdt db, int oldVersion, int newVersion)?
        onUpgrade,
  }) async {
    // Initialize FFI
    sqfliteFfiInit();
    if (Platform.isLinux) {
      await databaseFactoryFfi.setDatabasesPath('.');
    }

    var created = false;
    int? upgradeFrom;
    final db = await databaseFactoryFfi.openDatabase(
      inMemory ? inMemoryDatabasePath : '$basePath/$name.db',
      options: SqfliteOpenDatabaseOptions(
        version: version,
        onCreate: (_, __) => created = true,
        onUpgrade: (_, from, __) => upgradeFrom = from,
      ),
    );

    // Get existing node id, or generate one
    final canonicalTime = await _lastModified(db);
    final crdt = SqliteCrdt._(db, canonicalTime ?? Hlc.zero(Uuid().v4()));

    if (created) onCreate?.call(crdt, version ?? 1);
    if (upgradeFrom != null) onUpgrade?.call(crdt, upgradeFrom!, version ?? 1);

    return crdt;
  }

  var inTransaction = false;
  Hlc? _transactionCanonical;

  Future<void> execute(String sql, [List<Object?>? args]) async {
    final result = _sqlEngine.parse(sql);
    String? affectedTable;
    Hlc? executeCanonical;

    if (result.rootNode is InvalidStatement) {
      throw 'Unable to parse SQL statement';
    }

    // Inject CRDT columns into create statement
    if (result.rootNode is CreateTableStatement) {
      final statement = result.rootNode as CreateTableStatement;
      affectedTable = statement.tableName;

      final newStatement = CreateTableStatement(
        tableName: statement.tableName,
        columns: [
          ...statement.columns,
          ColumnDefinition(
            columnName: 'is_deleted',
            typeName: 'INTEGER',
            constraints: [Default(null, NumericLiteral(0))],
          ),
          ColumnDefinition(
            columnName: 'hlc',
            typeName: 'TEXT',
            constraints: [NotNull(null)],
          ),
          ColumnDefinition(
            columnName: 'modified',
            typeName: 'TEXT',
            constraints: [NotNull(null)],
          ),
        ],
        tableConstraints: statement.tableConstraints,
        ifNotExists: statement.ifNotExists,
        isStrict: statement.isStrict,
        withoutRowId: statement.withoutRowId,
      );

      sql = newStatement.toSql();
    }

    if (result.rootNode is InsertStatement) {
      final statement = result.rootNode as InsertStatement;
      affectedTable = statement.table.tableName;

      final newStatement = InsertStatement(
        table: statement.table,
        targetColumns: [
          ...statement.targetColumns,
          Reference(columnName: 'hlc'),
          Reference(columnName: 'modified'),
        ],
        source: ValuesSource([
          Tuple(expressions: [
            ...(statement.source as ValuesSource).values.first.expressions,
            NumberedVariable(null),
            NumberedVariable(null),
          ])
        ]),
      );

      sql = newStatement.toSql();

      executeCanonical = Hlc.send(canonicalTime);
      args?.addAll([canonicalTime, canonicalTime]);
    }

    if (result.rootNode is UpdateStatement) {
      final statement = result.rootNode as UpdateStatement;
      affectedTable = statement.table.tableName;

      var argCount = args?.length ?? 0;
      sql = UpdateStatement(
        table: statement.table,
        set: [
          ...statement.set,
          SetComponent(
            column: Reference(columnName: 'hlc'),
            expression: NumberedVariable(++argCount),
          ),
          SetComponent(
            column: Reference(columnName: 'modified'),
            expression: NumberedVariable(++argCount),
          ),
        ],
        where: statement.where,
      ).toSql();

      executeCanonical = Hlc.send(canonicalTime);
      args?.addAll([canonicalTime, canonicalTime]);
    }

    if (result.rootNode is DeleteStatement) {
      final statement = result.rootNode as DeleteStatement;
      affectedTable = statement.table.tableName;

      var argCount = args?.length ?? 0;
      sql = UpdateStatement(
        table: statement.table,
        set: [
          SetComponent(
            column: Reference(columnName: 'is_deleted'),
            expression: NumberedVariable(++argCount),
          ),
          SetComponent(
            column: Reference(columnName: 'hlc'),
            expression: NumberedVariable(++argCount),
          ),
          SetComponent(
            column: Reference(columnName: 'modified'),
            expression: NumberedVariable(++argCount),
          ),
        ],
        where: statement.where,
      ).toSql();

      executeCanonical = Hlc.send(canonicalTime);
      args = [...args ?? [], true, canonicalTime, canonicalTime];
    }

    if (result.rootNode is BeginTransactionStatement) {
      inTransaction = true;
    }

    if (result.rootNode is CommitStatement) {
      inTransaction = false;
      executeCanonical = _transactionCanonical;
      _transactionCanonical = null;
    }

    await _db.execute(sql, args?.map(_encode).toList());

    if (!inTransaction && executeCanonical != null) {
      canonicalTime = executeCanonical;
    } else {
      _transactionCanonical = executeCanonical;
    }

    if (affectedTable != null) await _onDbChanged({affectedTable});
  }

  Future<List<Map<String, Object?>>> query(String sql, [List<Object?>? args]) =>
      _db.rawQuery(sql, args?.map(_encode).toList());

  Stream<List<Map<String, Object?>>> watch(String sql,
      [List<Object?> Function()? args]) {
    late final StreamController<List<Map<String, Object?>>> controller;
    controller = StreamController<List<Map<String, Object?>>>(
      onListen: () {
        final query = _Query(sql, args?.call());
        _watches[controller] = query;
        _emitQuery(controller, query);
      },
      onCancel: () {
        _watches.remove(controller);
        controller.close();
      },
    );

    return controller.stream;
  }

  /// Returns all CRDT records in the database.
  /// Use [fromTables] to specify from which tables to read, returns all tables if null.
  /// Use [modifiedSince] to fetch only recently changed records.
  /// Set [onlyModifiedHere] to get only records changed in this node.
  Future<Map<String, Iterable<Map<String, Object?>>>> getChangeset(
          {Iterable<String>? fromTables,
          Hlc? modifiedSince,
          bool onlyModifiedHere = false}) async =>
      {
        for (final table in fromTables ?? await _getTables(_db))
          table: await _getTableChangeset(table)
      };

  Future<Iterable<Map<String, Object?>>> _getTableChangeset(String table,
      {Hlc? modifiedSince, bool onlyModifiedHere = false}) async {
    final conditions = [
      if (modifiedSince != null) "modified > '$modifiedSince'",
      if (onlyModifiedHere) "hlc LIKE '%$nodeId'",
    ];
    final conditionClause =
        conditions.isEmpty ? '' : 'WHERE ${conditions.join(' AND ')}';

    return await _db.rawQuery('SELECT * FROM $table $conditionClause');
  }

  /// Merge [changeset] into database
  Future<void> merge(
      Map<String, Iterable<Map<String, Object?>>> changeset) async {
    await _db.execute('BEGIN TRANSACTION');

    // Iterate through all the remote timestamps to
    // 1. Check for invalid entries (throws exception)
    // 2. Update local canonical time if needed
    var hlc = canonicalTime;
    for (final records in changeset.values) {
      hlc = records.fold<Hlc>(hlc,
          (hlc, record) => Hlc.recv(hlc, Hlc.parse(record['hlc'] as String)));
    }
    canonicalTime = hlc;

    for (final entry in changeset.entries) {
      final table = entry.key;
      final records = entry.value;

      for (final record in records) {
        record['modified'] = canonicalTime;

        final columns = record.keys.join(', ');
        final placeholders =
            List.generate(record.length, (i) => '?${i + 1}').join(', ');
        final values = record.values.map(_encode).toList();

        var i = 1;
        final updateStatement =
            record.keys.map((e) => '$e = ?${i++}').join(', \n');

        final sql = '''
          INSERT INTO $table ($columns)
            VALUES ($placeholders)
          ON CONFLICT DO
            UPDATE SET $updateStatement
          WHERE excluded.hlc > $table.hlc
        ''';
        await _db.execute(sql, values);
      }
    }

    await _db.execute('COMMIT TRANSACTION');
    await _onDbChanged(changeset.keys);
  }

  Future<void> _onDbChanged(Iterable<String> affectedTables) async {
    for (final entry in _watches.entries.toList()) {
      final controller = entry.key;
      final query = entry.value;
      if (affectedTables
          .firstWhere((e) => query.affectedTables.contains(e), orElse: () => '')
          .isNotEmpty) {
        await _emitQuery(controller, query);
      }
    }
  }

  Future<void> _emitQuery(
      StreamController<List<Map<String, dynamic>>> controller,
      _Query query) async {
    final result =
        await _db.rawQuery(query.sql, query.args?.map(_encode).toList());
    if (!controller.isClosed) {
      controller.add(result);
    }
  }
}

Object? _encode(Object? value) {
  if (value == null) return null;
  if (value is Map) return jsonEncode(value);
  if (value is Enum) return value.name;

  switch (value.runtimeType) {
    case String:
    case int:
    case double:
      return value;
    case bool:
      return (value as bool) ? 1 : 0;
    case DateTime:
      return (value as DateTime).toUtc().toIso8601String();
    case Hlc:
      return value.toString();
    default:
      throw 'Unsupported type: ${value.runtimeType}';
  }
}

class _Query {
  final String sql;
  final List<Object?>? args;
  final Set<String> affectedTables;

  _Query(this.sql, this.args)
      : affectedTables = _getAffectedTables(_sqlEngine.parse(sql).rootNode);

  static Set<String> _getAffectedTables(AstNode node) {
    if (node is TableReference) return {node.tableName};
    return node.allDescendants
        .fold({}, (prev, e) => prev..addAll(_getAffectedTables(e)));
  }
}
