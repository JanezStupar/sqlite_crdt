import 'dart:io';

const sqliteCrdtIsWeb = false;
final sqliteCrdtIsLinux = Platform.isLinux;

String sqliteCrdtResolvePath(String path) =>
    sqliteCrdtIsLinux && path != ':memory:' && !path.startsWith('file:')
    ? File(path).absolute.path
    : path;
