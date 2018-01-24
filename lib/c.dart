import "dart:typed_data";
import "dart:convert";
import "dart:async";
import "dart:math";
import "dart:io";

class KException implements Exception {
  final String message;

  KException([this.message = ""]) {
    print("New KException: $message");
  }

  @override
  String toString() {
    return 'KException{message: $message}';
  }
}

String i2(int i) {
  String s = i.toString();
  if (s.length < 2) s = "0" + s;
  return s;
}

String nine0 = "000000000";

String i9(int i) {
  String s = i.toString();
  if (s.length < 9) s = nine0.substring(0, 9 - s.length) + s;
  return s;
}

class Month {
  int i; // Number of months since Jan 2000

  Month(this.i);

  @override
  String toString() {
    int m = i + 24000;
    int y = m ~/ 12;
    return i == c.ni ? "" : i2(y ~/ 100) + i2(y % 100) + "-" + i2(1 + m % 12);
  }
}

class Minute {
  int i; // Number of minutes passed

  Minute(this.i);

  @override
  String toString() {
    return i == c.ni ? "" : i2(i ~/ 60) + ":" + i2(i % 60);
  }
}

class Timespan {
  /**
   * Number of nanoseconds passed.
   */
  int j;

  Timespan(this.j);

  String toString() {
    if (j == c.nj) return "";
    String s = j < 0 ? "-" : "";
    int jj = j < 0 ? -j : j;
    int d = (jj ~/ 86400000000000);
    if (d != 0) s += d.toString() + "D";
    return s +
        i2(((jj % 86400000000000) ~/ 3600000000000)) +
        ":" +
        i2(((jj % 3600000000000) ~/ 60000000000)) +
        ":" +
        i2(((jj % 60000000000) ~/ 1000000000)) +
        "." +
        i9((jj % 1000000000));
  }
}

class Second {
  /**
   * Number of seconds passed.
   */
  int i;

  Second(this.i);

  @override
  String toString() {
    return i == c.ni ? "" : new Minute(i ~/ 60).toString() + ':' + i2(i % 60);
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Second && runtimeType == other.runtimeType && i == other.i;

  @override
  int get hashCode => i.hashCode;
}

class Character {}

class Flip {
  List<String> x;
  List y;

  Flip(this.x, this.y);

  factory Flip.fromDict(Dict X) {
    return new Flip(X.x, X.y);
  }
}

class Dict {
  dynamic x, y;

  Dict(this.x, this.y);
}

class DataInputStream {
  Stream<List<int>> s;
  var dataWaiter = (int) {};

  DataInputStream(this.s) {
    runReader();
  }

  List<List<int>> input = new List();
  int nInput = 0;

  runReader() async {
    await for (var data in s) {
      //print("Socket read: ${data.length} bytes");
      input.add(new List()..addAll(data));
      nInput += data.length;
      dataWaiter(nInput);
    }
  }

  Future<int> readByte() async {
    await ensure(1);
    nInput--;
    var retval = input.first.removeAt(0);
    removeEmpty();
    return retval;
  }

  Future<ByteData> readBytes(int nbytes) async {
    await ensure(nbytes);
    ByteData retval = new ByteData(nbytes);
    int destix = 0;
    while (true) {
      var ifirst = input[0];
      if (ifirst.length >= nbytes) {
        for (int q = 0; q < nbytes; q++) {
          retval.setUint8(q + destix, ifirst[q]);
        }
        nInput -= nbytes;
        input[0] = ifirst.sublist(nbytes, nbytes + ifirst.length - nbytes);
        removeEmpty();
        break;
      } else {
        int lim = ifirst.length;
        for (int q = 0; q < lim; q++) {
          retval.setUint8(destix + q, ifirst[q]);
        }
        nbytes -= lim;
        destix += lim;
        nInput -= lim;
        input.removeAt(0);
      }
    }
    return retval;
  }

  removeEmpty() {
    while (!input.isEmpty && input.first.isEmpty) {
      input.removeAt(0);
    }
  }

  Future<int> ensure(int size) {
    if (available() >= size) return new Future.value(available());
    Completer<int> completer = new Completer<int>();
    dataWaiter = (sz) {
      if (sz >= size) {
        completer.complete(sz);
      }
    };
    return completer.future;
  }

  int available() {
    return nInput;
  }
}

class DataOutputStream {
  IOSink s;

  DataOutputStream(this.s);

  write(ByteData b) async {
    s.add(b.buffer.asInt8List());
  }
}

class c {
  Socket x;
  DataInputStream i;
  DataOutputStream o;
  bool compression = false;
  int J; // output buffer sz
  int j; // input buffer ptr
  bool a; // endianness
  int sync = 0;
  int vt;
  ByteData B; // output buffer
  ByteData b; // input buffer

  connect(String host, int port) async {
    String usernamepassword = "user"; // username:password
    B = new ByteData(2 + ns(usernamepassword));
    var s = await Socket.connect(host, port);
    await io(s);
    J = 0;
    wstring(usernamepassword + "\u0003");
    await o.write(B);
    int maybeVersion = -1;
    try {
      maybeVersion = await i.readByte();
    } catch (e) {
      await close();
      B = new ByteData(1 + ns(usernamepassword));
      await io(await Socket.connect(host, port));
      J = 0;
      wstring(usernamepassword);
      await o.write(B);
      try {
        maybeVersion = await i.readByte();
      } catch (e) {
        await close();
        throw new KException("access");
      }
    }
    vt = min(maybeVersion, 3);
  }

  wshort(int q) {
    wbyte((q >> 8) & 0xFF);
    wbyte(q & 0xFF);
  }

  wint(int q) {
    wshort(q >> 16);
    wshort(q);
  }

  wlong(int q) {
    wint(q >> 32);
    wint(q);
  }

  wboolean(bool q) {
    wbyte(q ? 1 : 0);
  }

  ByteData bytedata8 = new ByteData(8);

  wdouble(double q) {
    bytedata8.setFloat64(0, q);
    int l = bytedata8.getInt64(0);
    wlong(l);
  }

  wchar(String q) {
    unimplemented("char serialize");
  }

  int intType(int t) {
    if (t == ni) return -6; // null int
    if (t == nj) return -7; // null long
    if (t >= -128 && t <= 127) return -4; // byte
    if (t >= -32768 && t <= 32767) return -5; // short
    if (t >= -0x800000 && t <= 0x7FFFFFFF) return -6; // int 4 byte
    return -7; // long
  }

  bool isInt(int t) {
    return t >= 4 && t <= 7;
  }

  int listType(List q) {
    if (q.isEmpty) return 0; // empty generic list.
    var typ = -1000;
    for (var v in q) {
      var t = tany(v);
      if (typ == -1000) {
        typ = t;
        continue;
      }
      if (t != typ) {
        if (isInt(typ) && isInt(t)) {
          typ = min(t, typ);
        } else {
          return 0;
        }
      } else {
        return 0;
      }
    }
    if (typ == -1000) typ = 0;
    if (typ > 0) return 0; // list of lists
    return -typ;
  }

  int tany(dynamic x) {
    return x is bool
        ? -1
        : x is int
            ? intType(x)
            : x is double
                ? -9
                : x is Character
                    ? -10
                    : x is String
                        ? -11
                        : x is DateTime
                            ? -15
                            : x is Timespan
                                ? -16
                                : x is Month
                                    ? -13
                                    : x is Minute
                                        ? -17
                                        : x is Second
                                            ? -18
                                            : x is ByteData
                                                ? 10
                                                : x is List
                                                    ? listType(x)
                                                    : x is Flip
                                                        ? 98
                                                        : x is Dict ? 99 : 0;
  }

  wany(dynamic x) {
    int i = 0, n, t = tany(x);
    wbyte(t);
    if (t < 0) {
      switch (t) {
        case -1:
          wboolean(x);
          return;
        case -4:
          wbyte(x);
          return;
        case -5:
          wshort(x);
          return;
        case -6:
          wint(x);
          return;
        case -7:
          wlong(x);
          return;
        case -9:
          wdouble(x);
          return;
        case -10:
          wchar(x);
          return;
        case -11:
          wstring(x);
          return;
        case -13:
          wmonth(x);
          return;
        case -15:
          wdatetime(x);
          return;
        case -16:
          wtimespan(x);
          return;
        case -17:
          wminute(x);
          return;
        case -18:
          wsecond(x);
          return;
      }
    }
    if (t == 99) {
      var d = x as Dict;
      wany(d.x);
      wany(d.y);
      return;
    }
    wbyte(0);
    if (t == 98) {
      Flip r = x as Flip;
      wbyte(99);
      wany(r.x);
      wany(r.y);
      return;
    }
    if (t == 10 && x is ByteData) {
      var byteData = (x as ByteData);
      wint(byteData.lengthInBytes);
      wbytes(byteData.buffer.asUint8List());
      return;
    }
    List xl = x as List;
    wint(n = x.length);
    if (t == 10) {
      unimplemented("Char array serialize");
    } else {
      for (i = 0; i < n; i++) {
        switch (t) {
          case 0:
            wany(xl[i]);
            break;
          case 1:
            wboolean(xl[i]);
            break;
          case 4:
            wbyte(xl[i]);
            break;
          case 5:
            wshort(xl[i]);
            break;
          case 6:
            wint(xl[i]);
            break;
          case 7:
            wlong(xl[i]);
            break;
          case 9:
            wdouble(xl[i]);
            break;
          case 10:
            wchar(xl[i]);
            break;
          case 11:
            wstring(xl[i]);
            break;
          case 13:
            wmonth(xl[i]);
            break;
          case 15:
            wdatetime(xl[i]);
            break;
          case 16:
            wtimespan(xl[i]);
            break;
          case 17:
            wminute(xl[i]);
            break;
          case 18:
            wsecond(xl[i]);
            break;
        }
      }
    }
  }

  int nelems(dynamic x) {
    return x is Dict
        ? nelems((x as Dict).x)
        : x is Flip
            ? nelems((x as Flip).y[0])
            : x is String
                ? ns(x)
                : (x is ByteData)
                    ? (x as ByteData).lengthInBytes
                    : (x as List).length;
  }

  static List<int> nt = [
    0,
    1,
    16,
    0,
    1,
    2,
    4,
    8,
    4,
    8,
    1,
    0,
    8,
    4,
    4,
    8,
    8,
    4,
    4,
    4
  ];

  int nxDynamic(dynamic x) {
    int i = 0, n, t = tany(x), j;
    if (t == 99) return 1 + nxDynamic((x as Dict).x) + nxDynamic((x as Dict).y);
    if (t == 98) return 3 + nxDynamic((x as Flip).x) + nxDynamic((x as Flip).y);
    if (t < 0) return t == -11 ? 2 + ns(x as String) : 1 + nt[-t];
    j = 6;
    n = nelems(x);
    if (t == 0 || t == 11)
      for (; i < n; ++i)
        j += t == 0 ? nxDynamic((x as List)[i]) : 1 + ns((x as List)[i]);
    else
      j += n * nt[t];
    return j;
  }

  ByteData serializeMsg(int msgType, dynamic x) {
    int len = 8 + nxDynamic(x);
    B = new ByteData(len);
    B.setInt8(0, 0);
    B.setInt8(1, msgType);
    J = 4;
    wint(len);
    wany(x);
    return B;
  }

  writeMsg(int type, dynamic stuff) async {
    var buffer = serializeMsg(type, stuff);
//    for(int i=0; i<buffer.lengthInBytes; i++) {
//      print("${i}: ${buffer.getUint8(i)}");
//    }
    await o.write(buffer);
  }

  int rshort() {
    int x = b.getUint8(j++);
    int y = b.getUint8(j++);
    int rv = (a ? (x & 0xff) | (y << 8) : (x << 8) | (y & 0xff));
    if (rv > 0x7FFF) {
      // 0x8000 .. 0xFFFF
      rv = rv - 0x10000;
    }
    return rv;
  }

  int rint() {
    int x = rshort(), y = rshort();
    return a ? x & 0xffff | y << 16 : x << 16 | y & 0xffff;
  }

  int rlong() {
    int x = rint(), y = rint();
    return a ? x & 0xffffffff | y << 32 : x << 32 | y & 0xffffffff;
  }

  double rfloat() {
    int x = rint();
    bytedata8.setInt32(0, x);
    return bytedata8.getFloat32(0);
  }

  double rdouble() {
    int x = rlong();
    bytedata8.setInt64(0, x);
    return bytedata8.getFloat64(0);
  }

  String rstring() {
    int i = j;
    for (; b.getInt8(j++) != 0;);
    return (i == j - 1)
        ? ""
        : utf8codec.decode(b.buffer.asUint8List(i, j - 1 - i).toList());
  }

  DateTime rtimestamp() {
    int j = rlong(), d = j < 0 ? (j + 1) ~/ n - 1 : j ~/ n;
    int millis = j != nj ? (j ~/ 1000000) % 1000 : 0;
    DateTime p = new DateTime.fromMillisecondsSinceEpoch(
        j == nj ? j : (k + 1000 * d + millis),
        isUtc: true);
    return p;
  }

  Month rmonth() {
    return new Month(rint());
  }

  DateTime rdate() {
    int i = rint();
    return new DateTime.fromMillisecondsSinceEpoch(
        i == ni ? nj : (k + 86400000 * i),
        isUtc: true);
  }

  DateTime rdate2() {
    double f = rdouble();
    return new DateTime.fromMillisecondsSinceEpoch(
        f.isNaN ? 0 : (k + (8.64e7 * f).round()),
        isUtc: true);
  }

  Timespan rtimespan() {
    return new Timespan(rlong());
  }

  Minute rminute() {
    return new Minute(rint());
  }

  Second rsecond() {
    return new Second(rint());
  }

  DateTime rtime() {
    int i = rint();
    return new DateTime.fromMillisecondsSinceEpoch(i == ni ? nj : i,
        isUtc: true);
  }

  bool rbool() {
    return b.getInt8(j++) != 0;
  }

  int rchar() {
    return b.getInt8(j++);
  }

  dynamic rany() {
    int i = 0, n, t = b.getInt8(j++);
    if (t < 0) {
      switch (t) {
        case -1:
          return rbool();
        case -2:
          unimplemented("Read UUID not implemented");
          break;
        case -4:
          return b.getInt8(j++);
        case -5:
          return rshort();
        case -6:
          return rint();
        case -7:
          return rlong();
        case -8:
          return rfloat();
        case -9:
          return rdouble();
        case -10:
          return rchar();
        case -11:
          return rstring();
        case -12:
          return rtimestamp();
        case -13:
          return rmonth();
        case -14:
          return rdate();
        case -15:
          return rdate2();
        case -16:
          return rtimespan();
        case -17:
          return rminute();
        case -18:
          return rsecond();
        case -19:
          return rtime();
      }
    }
    if (t > 99) {
      if (t == 100) {
        rstring();
        return rany();
      }
      if (t < 104) return b.getInt8(j++) == 0 && t == 101 ? null : "func";
      if (t > 105)
        rany();
      else
        for (n = rint(); i < n; i++) rany();
      return "func";
    }
    if (t == 99) return new Dict(rany(), rany());
    j++;
    if (t == 98) return new Flip.fromDict(rany() as Dict);
    n = rint();
    switch (t) {
      case 0:
        List L = new List(n);
        for (; i < n; i++) L[i] = rany();
        return L;
      case 1:
        List<bool> B = new List<bool>(n);
        for (; i < n; i++) B[i] = rbool();
        return B;
      case 2:
        unimplemented("Reading array of guids");
        break;
      case 4:
        List<int> L = new List<int>(n);
        for (; i < n; i++) L[i] = b.getInt8(j++);
        return L;
      case 5:
        List<int> L = new List<int>(n);
        for (; i < n; i++) L[i] = rshort();
        return L;
      case 6:
        List<int> L = new List<int>(n);
        for (; i < n; i++) L[i] = rint();
        return L;
      case 7:
        List<int> L = new List<int>(n);
        for (; i < n; i++) L[i] = rlong();
        return L;
      case 8:
        List<double> L = new List<double>(n);
        for (; i < n; i++) L[i] = rfloat();
        return L;
      case 9:
        List<double> L = new List<double>(n);
        for (; i < n; i++) L[i] = rdouble();
        return L;
      case 10:
        var str = utf8codec.decode(b.buffer.asUint8List(j, n).toList());
        j += n;
        return str;
      case 11:
        List<String> L = new List<String>(n);
        for (; i < n; i++) L[i] = rstring();
        return L;
      case 12:
        List<DateTime> L = new List<DateTime>(n);
        for (; i < n; i++) L[i] = rtimestamp();
        return L;
      case 13:
        List<Month> L = new List<Month>(n);
        for (; i < n; i++) L[i] = rmonth();
        return L;
      case 14:
        List<DateTime> L = new List<DateTime>(n);
        for (; i < n; i++) L[i] = rdate();
        return L;
      case 15:
        List<DateTime> L = new List<DateTime>(n);
        for (; i < n; i++) L[i] = rdate2();
        return L;
      case 16:
        List<Timespan> L = new List<Timespan>(n);
        for (; i < n; i++) L[i] = rtimespan();
        return L;
      case 17:
        List<Minute> L = new List<Minute>(n);
        for (; i < n; i++) L[i] = rminute();
        return L;
      case 18:
        List<Second> L = new List<Second>(n);
        for (; i < n; i++) L[i] = rsecond();
        return L;
      case 19:
        List<DateTime> L = new List<DateTime>(n);
        for (; i < n; i++) L[i] = rtime();
        return L;
    }
    return null;
  }

  dynamic deserializeAny() {
    if (b.getInt8(0) == -128) {
      j++;
      throw new KException(rstring());
    }
    return rany(); // deserialize the message
  }

  uncompress() {
    int n = 0, r = 0, f = 0, s = 8, p = s;
    int i = 0;
    var dst = new ByteData(rint());
    int d = j;
    var aa = new List<int>(256);
    while (s < dst.lengthInBytes) {
      if (i == 0) {
        f = b.getUint8(d++);
        i = 1;
      }
      if ((f & i) != 0) {
        r = aa[b.getUint8(d++)];
        dst.setUint8(s++, dst.getUint8(r++));
        dst.setUint8(s++, dst.getUint8(r++));
        n = b.getUint8(d++);
        for (int m = 0; m < n; m++) dst.setUint8(s + m, dst.getUint8(r + m));
      } else {
        dst.setUint8(s++, b.getUint8(d++));
      }
      while (p < s - 1)
        aa[dst.getUint8(p) ^ dst.getUint8(p + 1)] = p++;
      if ((f & i) != 0) p = s += n;
      i *= 2;
      if (i == 256)
        i = 0;
    }
    b = dst;
    j = 8;
  }

  Future<dynamic> parseResponse() async {
    b = await i.readBytes(8);
    a = b.getInt8(0) == 1; // endianness of the msg
    if (b.getInt8(1) == 1) // msg types are 0 - async, 1 - sync, 2 - response
      sync++; // an incoming sync message means the remote will expect a response message
    j = 4;
    var ilen = rint();
    var compressed = b.getInt8(2) == 1;
    b = await i.readBytes(ilen - 8);
    j = 0;
    if (compressed) {
      uncompress();
      j = 8;
    }
    var retval = deserializeAny();
    return retval;
  }

  Future<dynamic> execAny(dynamic list) async {
    await writeMsg(1, list);
    return await parseResponse();
  }

  Future<dynamic> execFun(String str, List args) {
    var byteData =
        new ByteData.view(new Int8List.fromList(utf8codec.encode(str)).buffer);
    return execAny(new List()
      ..add(byteData)
      ..addAll(args));
  }

  Future<dynamic> exec(String str) {
    var byteData =
        new ByteData.view(new Int8List.fromList(utf8codec.encode(str)).buffer);
    return execAny(byteData);
  }

  close() async {
    if (null != x) {
      await x.close();
      x = null;
    }
    i = null;
    o = null;
  }

  void wstring(String s) {
    int i = 0, n;
    if (s != null) {
      n = ns(s);
      var b = utf8codec.encode(s);
      for (; i < n;) wbyte(b[i++]);
    }
    wbyte(0);
  }

  void wmonth(Month m) {
    wint(m.i);
  }

  void wminute(Minute m) {
    wint(m.i);
  }

  void wsecond(Second m) {
    wint(m.i);
  }

  void wbyte(int x) {
    if (x >= 0) {
      B.setUint8(J++, x);
    } else {
      B.setInt8(J++, x);
    }
  }

  void wbytes(Uint8List x) {
    int limit = x.length;
    for (int i = 0; i < limit; i++) {
      B.setUint8(J++, x[i]);
    }
  }

  static double nf = double.NAN;
  static int nj = 0x8000000000000000;
  static int ni = 0x80000000;
  static int k = 86400000 * 10957;
  static int n = 1000000000;

  void wdatetime(DateTime dt) {
    int j = dt.millisecondsSinceEpoch;
    wdouble(j == 0 ? nf : (j - k) / 8.64e7);
  }

  void wtimespan(Timespan t) {
    if (vt < 1) unimplemented("Timespan not valid pre kdb+2.6");
    wlong(t.j);
  }

  io(Socket s) async {
    x = s;
    s.setOption(SocketOption.TCP_NODELAY, true);
    i = new DataInputStream(s);
    o = new DataOutputStream(s);
  }

  static Utf8Codec utf8codec = new Utf8Codec();

  static int ns(String s) {
    int i;
    if (s == null) return 0;
    if (-1 < (i = s.indexOf('\u0000'))) s = s.substring(0, i);
    return utf8codec.encode(s).length;
  }

  unimplemented(String msg) {
    throw new UnimplementedError(msg);
  }
}
