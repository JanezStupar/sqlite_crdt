import 'dart:async';

import 'package:sqflite_common/sqlite_api.dart';
// ignore: implementation_imports
import 'package:sqflite_common/src/database_mixin.dart';
// ignore: implementation_imports
import 'package:sqflite_common/src/transaction.dart';
import 'package:sql_crdt/sql_crdt.dart';

class ExecutorApi extends DatabaseApi {
  final DatabaseExecutor _db;

  ExecutorApi(this._db);

  @override
  Future<ExecuteResult> execute(String sql, [List<Object?>? args]) async {
    // Use rawQuery to support RETURNING clauses in modern SQLite (3.35.0+)
    final result = await _db.rawQuery(sql, args);
    return result.isEmpty ? const VoidResult() : QueryResult(result, sql: sql);
  }

  @override
  Future<List<Map<String, Object?>>> query(String sql, [List<Object?>? args]) =>
      _db.rawQuery(sql, args);

  @override
  Future<void> transaction(
    Future<void> Function(ReadWriteApi api) actions,
  ) async {
    assert(_db is Database, 'Cannot start a transaction within a transaction');
    final database = _db as Database;
    var actionsCompleted = false;

    try {
      await database.transaction((t) async {
        await actions(ExecutorApi(t));
        actionsCompleted = true;
      });
    } catch (error, stackTrace) {
      if (!actionsCompleted) {
        Error.throwWithStackTrace(error, stackTrace);
      }

      // sqflite marks its transaction object closed even when COMMIT fails.
      // With deferred constraints, the native SQLite transaction can still be
      // active, leaving subsequent operations blocked. Force a rollback before
      // propagating the original commit error.
      final databaseMixin = database as SqfliteDatabaseMixin;
      try {
        await databaseMixin.invokeExecute<dynamic>(
          getForcedSqfliteTransaction(databaseMixin),
          'ROLLBACK',
          null,
          inTransactionChange: false,
        );
      } on DatabaseException catch (rollbackError, rollbackStackTrace) {
        // A backend can report a COMMIT failure after it has already ended the
        // transaction. In that case the original commit error remains the
        // useful failure and there is nothing left to recover.
        if (!_isNoTransactionActive(rollbackError)) {
          Error.throwWithStackTrace(
            _TransactionRecoveryException(error, rollbackError),
            rollbackStackTrace,
          );
        }
      } finally {
        databaseMixin.inTransaction = false;
      }

      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  @override
  Future<void> executeBatch(
    FutureOr<void> Function(WriteApi api) actions,
  ) async {
    final batch = _db.batch();
    actions(BatchApi(batch));
    await batch.commit();
  }
}

bool _isNoTransactionActive(DatabaseException error) =>
    error.toString().toLowerCase().contains('no transaction is active');

final class _TransactionRecoveryException implements Exception {
  final Object transactionError;
  final Object rollbackError;

  const _TransactionRecoveryException(
    this.transactionError,
    this.rollbackError,
  );

  @override
  String toString() =>
      'Transaction failed with $transactionError and rollback failed with '
      '$rollbackError';
}

/// Simplified wrapper intended for Sqlite batches.
class BatchApi extends WriteApi {
  final Batch _batch;

  BatchApi(this._batch);

  void query(String sql, [List<Object?>? args]) => _batch.rawQuery(sql, args);

  @override
  ExecuteResult execute(String sql, [List<Object?>? args]) {
    // Use rawQuery to support RETURNING clauses in modern SQLite (3.35.0+)
    // Note: Batch operations queue results until batch.commit() is called
    _batch.execute(sql, args);
    return const VoidResult();
  }
}
