import "package:kdb/c.dart";
import "package:test/test.dart";
import 'package:collection/collection.dart';

void main() {

  c conn;

  setUp(() async {
    conn = new c();
    await conn.connect('localhost', 5000);
  });

  tearDown(() async {
    conn.close();
    conn = null;
  });

  test("long test", () async {
    expect(await conn.exec("2+3"), equals(5));
  });

  test("long test2", () async {
    expect(await conn.execFun("{x}",[0xFFFFFFFFAA]), equals(0xFFFFFFFFAA));
  });

  test("long test3", () async {
    expect(await conn.execFun("{type x}",[0xFFFFFFFFAA]), equals(-7));
  });

  test("short test", () async {
    expect(await conn.exec("2h+3h"), equals(5));
  });

  test("short test2", () async {
    expect(await conn.execFun("{x}",[0x6FAA]), equals(0x6FAA));
  });

  test("short test3", () async {
    expect(await conn.execFun("{type x}",[0x6FAA]), equals(-5));
  });

  test("int test", () async {
    expect(await conn.exec("2i+3i"), equals(5));
  });

  test("int test2", () async {
    expect(await conn.execFun("{x}",[0x6FFFFFAA]), equals(0x6FFFFFAA));
  });

  test("int test3", () async {
    expect(await conn.execFun("{type x}",[0x6FFFFFAA]), equals(-6));
  });

  test("string test", () async {
    expect(await conn.exec("\"hel\",\"lo\""), equals("hello"));
  });

  test("sym test", () async {
    expect(await conn.exec("`morra"), equals("morra"));
  });

  test("sym pass test", () async {
    expect(await conn.execFun("{type x}",["hello"]), equals(-11));
  });

  test("3 args test", () async {
    expect(await conn.execFun("{x+y+z}",[1,2,3]), equals(6));
  });

  test("date2 test", () async {
    var dt = await conn.exec("2017.01.01T23:59:59.123");
    expect(dt.toString(), equals("2017-01-01 23:59:59.123Z"));
  });

  test("date test", () async {
    var dt = await conn.exec("2017.01.01D23:59:59.123");
    expect(dt.toString(), equals("2017-01-01 23:59:59.123Z"));
    var dt2 = await conn.execFun("{x}",[dt]);
    expect(dt2.toString(), equals("2017-01-01 23:59:59.123Z"));
  });

  test("year test", () async {
    var dt = await conn.exec("2017.01.01");
    expect(dt.toString(), equals("2017-01-01 00:00:00.000Z"));
    var dt2 = await conn.execFun("{x}",[dt]);
    expect(dt2.toString(), equals("2017-01-01 00:00:00.000Z"));
  });

  test("select/flip test", () async {
    await conn.exec("tab:flip `items`sales`prices`ts!(`nut`bolt`cam`cog;6 8 0 3;10 20 15 200000000;2017.01.01T23:59:59.123 2017.01.01T23:59:59.124 2017.01.01T23:59:59.125 2017.01.01T23:59:59.126);");
    Flip tab = await conn.exec("select from tab");
    await conn.execFun("{tab2::x;}", [tab]);
    Flip tab2 = await conn.exec("select from tab2");
    expect(const ListEquality().equals(tab.x, tab2.x), equals(true));
    expect(const DeepCollectionEquality().equals(tab.y, tab2.y), equals(true));
  });

  test("dict test", () async {
    var dict = await conn.exec("`items`sales`prices`ts!(`nut`bolt`cam`cog;6 8 0 3;10 20 15 200000000;2017.01.01T23:59:59.123 2017.01.01T23:59:59.124 2017.01.01T23:59:59.125 2017.01.01T23:59:59.126)");
    expect(dict is Dict, equals(true));
    var dict2 = await conn.execFun("{x}",[dict]);
    expect(dict2 is Dict, equals(true));
    expect(const DeepCollectionEquality().equals(dict.y, dict2.y), equals(true));
    expect(const DeepCollectionEquality().equals(dict.x, dict2.x), equals(true));
  });

  test("list", () async {
    var lst = await conn.exec("(1;\"a\";\"xxxx\";`zorro;2019.01.01)");
    expect(lst is List, equals(true));
    var lst2 = await conn.execFun("{x}",[lst]);
    expect(lst2 is List, equals(true));
    expect(const DeepCollectionEquality().equals(lst, lst2), equals(true));
  });

  test("exceptions", () async {
    try {
      await conn.exec("'exception1");
      expect(false, equals(true));
    } on KException {
      expect(true, equals(true));
    } catch(e2) {
      expect(false, equals(true));
    }
  });

}
