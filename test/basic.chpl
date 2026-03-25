use UnitTest;
use Parquet;
use TestUtil;

import Path;
import FileSystem as FS;

import BlockDist.blockDist;

config const n = 100;


proc testMultiColWriteRead(test: borrowed Test) throws {
  var Arr1, Arr2, Arr3: [1..10] int;
  Arr1 = 1;
  Arr2 = 2;
  Arr3 = 3;

  var In: [1..10] int;

  manage new tempDir() as temp {
    const filePath = Path.joinPath(temp.path,
                                   "testMultiColWriteRead.parquet");

    writeTable(filePath, colNames=("Arr1", "Arr2", "Arr3"),
               Arr1, Arr2, Arr3);

    readColumn(filePath, "Arr1", In);
    test.assertEqual(Arr1, In);
    readColumn(filePath, "Arr2", In);
    test.assertEqual(Arr2, In);
    readColumn(filePath, "Arr3", In);
    test.assertEqual(Arr3, In);
  }

  manage new tempDir() as temp {
    const doubleArr : [1..10] real = 42.0;
    const boolArr : [1..10] bool = true;
    const intArr : [1..10] int = 7;
    const uintArr : [1..10] uint = 8;

    const filePath = Path.joinPath(temp.path,
                                   "variousTypes.parquet");

    const names = ("DoubleArr", "BoolArr", "IntArr", "UintArr");
    writeTable(filePath, colNames=names,
                doubleArr, boolArr, intArr, uintArr);

    var doubleIn: [1..10] real;
    readColumn(filePath, "DoubleArr", doubleIn);
    test.assertEqual(doubleArr, doubleIn);

    var boolIn: [1..10] bool;
    readColumn(filePath, "BoolArr", boolIn);
    test.assertEqual(boolArr, boolIn);

    var intIn: [1..10] int;
    readColumn(filePath, "IntArr", intIn);
    test.assertEqual(intArr, intIn);

    var uintIn: [1..10] uint;
    readColumn(filePath, "UintArr", uintIn);
    test.assertEqual(uintArr, uintIn);
  }
}

proc testDistributedWriteRead(test: borrowed Test) throws {
  var ArrOut, ArrIn = blockDist.createArray(1..n, int);

  ArrOut = 2;

  manage new tempDir() as temp {
    const filePath = Path.joinPath(temp.path,
                                   "testDistributedWriteRead.parquet");

    write1DDistArrayParquet(filePath, "Arr", CompressionType.NONE, TRUNCATE,
                            ArrOut);

    test.assertTrue(FS.isFile(filePath));

    readColumn(filename=filePath, colName="Arr", Arr=ArrIn);

    test.assertEqual(ArrOut, ArrIn);
  }
}

proc testWriteRead(test: borrowed Test) throws {
  param val = 3;

  var ArrOut, ArrIn: [1..n] int;
  ArrOut = val;

  manage new tempDir() as temp {
    const filePath = Path.joinPath(temp.path,
                                   "testWriteRead.parquet");

    writeColumn(filename=filePath, colName="Arr", Arr=ArrOut);

    test.assertEqual(getNumCols(filePath), 1);
    test.assertEqual(getAllTypes(filePath)[0], ARROWINT64);

    readColumn(filename=filePath, colName="Arr", Arr=ArrIn);

    test.assertEqual(+ reduce ArrIn, val*n);
  }
}

proc testNumCols(test: borrowed Test) throws {
  const filename = "test/resources/multi-col.parquet";

  test.assertTrue(getNumCols(filename) == 3);
}

proc testTypes(test: borrowed Test) throws {
  const filename = "test/resources/multi-col.parquet";

  const types = getAllTypes(filename);

  test.assertEqual(types[0], ARROWINT64);
  test.assertEqual(types[1], ARROWBOOLEAN);
  test.assertEqual(types[2], ARROWINT64);
}

proc testReadColumn(test: borrowed Test) throws {
  const filename = "test/resources/multi-col.parquet";

}


UnitTest.main();
