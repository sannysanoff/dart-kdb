# dart-kdb

Driver for kx.com database (kdb+).

Implements some viable set of functionality. 

# usage

```
  var conn = new c();
  await conn.connect("localhost", 5000);
  var five = await conn.exec("2j+3j");
  var fifteen = await conn.execFun("{x+y}",[z, 10]);
  await conn.close()
```
