use UnitTest;
use Parquet;

import BlockDist.blockDist;

config const n = 100;


proc testFullWriteRead(test: borrowed Test) throws {
  var Arr = blockDist.createArray(1..n, int);

  Arr = 2;
  write1DDistArrayParquet("Arr.parquet", "Arr", ArrowTypes.int64:string,
                          CompressionType.NONE, TRUNCATE, Arr);
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


UnitTest.main();
