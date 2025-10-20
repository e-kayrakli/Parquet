use UnitTest;
use Parquet;

import BlockDist.blockDist;

config const n = 100;

var Arr = blockDist.createArray(1..n, int);

Arr = 2;


proc testFullWriteRead(test: borrowed Test) throws {
  write1DDistArrayParquet("Arr.parquet", "Arr", ArrowTypes.int64:string,
                          CompressionType.NONE, TRUNCATE, Arr);
}


UnitTest.main();
