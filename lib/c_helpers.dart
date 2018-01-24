import 'dart:async';
import 'package:kdb/c.dart';

/**
 *  for upsert into keyed table
 */
class KeyedTable {

  Flip ks;
  Flip cs;

  KeyedTable() {
  }

  factory KeyedTable.empty(List<String> keys, List<String> cols) {
    var t = new KeyedTable();
    t.ks = new Flip(new List(), new List());
    t.cs = new Flip(new List(), new List());
    for (var c in keys) {
      t.ks.x.add(c);
      t.ks.y.add(new List());
    }
    for (var c in cols) {
      t.cs.x.add(c);
      t.cs.y.add(new List());
    }
    return t;
  }

  addRow(List<dynamic> keys, List<dynamic> cols) {
    for (int i = keys.length - 1; i >= 0; i--) {
      (ks.y[i] as List).add(keys[i]);
    }
    for (int i = cols.length - 1; i >= 0; i--) {
      (cs.y[i] as List).add(cols[i]);
    }
  }

  Dict toDict() {
    return new Dict(ks, cs);
  }

}

class PlainTable {
  List<String> cols;
  List<List> vals;
  int nrows;

  PlainTable() {

  }

  factory PlainTable.fromFlip(Flip d) {
    var t = new PlainTable();
    t.cols = d.x;
    t.vals = d.y;
    t.nrows = t.vals[0].length;
    return t;
  }

  double getDouble(int col,int row) {
    return vals[col][row];
  }

  int getInt(int col,int row) {
    return vals[col][row];
  }

  String getString(int col,int row) {
    return vals[col][row];
  }

  DateTime getDateTime(int col,int row) {
    return vals[col][row];
  }

}

class ConnectionManager {
  String host;
  int port;

  ConnectionManager(this.host, this.port);

  dynamic runQ(Future<dynamic> block(c kdb)) async {
    var kdb = new c();
    var rv = null;
    try {
      await kdb.connect(host, port);
      return await block(kdb);
    } finally {
      await kdb.close();
    }
  }


}