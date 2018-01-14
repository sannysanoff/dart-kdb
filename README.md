# dart-kdb

Driver for kx.com database (kdb+). Implements some viable set of functionality. 

# types

* ints are serialized as bytes/shorts/ints/longs depending on range (signed).
* char arrays (strings), chars, syms are deserialized as strings, serialized as syms.
* dates,timestamps are deserialized as DateTime with millisecond precision, serialized as -15(dateTtime).
* floats and doubles are deserialized as doubles. Serialized as doubles.

# usage

```
  var conn = new c();
  await conn.connect("localhost", 5000);
  var five = await conn.exec("2j+3j");
  var fifteen = await conn.execFun("{x+y}",[z, 10]);
  await conn.close()
```
