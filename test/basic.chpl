use UnitTest;
use Parquet;
use TestUtil;

import Path;
import FileSystem as FS;

import BlockDist.blockDist;

config const n = 100;


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
