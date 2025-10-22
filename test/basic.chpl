use UnitTest;
use Parquet;

import BlockDist.blockDist;

config const n = 100;


proc testDistributedWriteRead(test: borrowed Test) throws {
  var ArrOut, ArrIn = blockDist.createArray(1..n, int);

  ArrOut = 2;
  write1DDistArrayParquet("testDistributedWriteRead.parquet", "Arr",
                          ArrowTypes.int64:string,
                          CompressionType.NONE, TRUNCATE, ArrOut);

  
}

proc testWriteRead(test: borrowed Test) throws {
  param val = 3;

  var ArrOut, ArrIn: [1..n] int;
  ArrOut = val;

  const filename = "testWriteRead.parquet";

  writeColumn(filename=filename, colName="Arr", Arr=ArrOut, dtype=ARROWINT64);

  test.assertEqual(getNumCols(filename), 1);
  test.assertEqual(getAllTypes(filename)[0], ARROWINT64);

  readColumn(filename=filename, colName="Arr", Arr=ArrIn);

  test.assertEqual(+ reduce ArrIn, val*n);
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
