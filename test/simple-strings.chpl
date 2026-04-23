// Copyright Hewlett Packard Enterprise Development LP.
use UnitTest;
use Parquet;
use TestUtil;

import Path;
import FileSystem as FS;

import BlockDist.blockDist;

config const n = 100;

proc testWriteRead(test: borrowed Test) throws {
  var Arr1, Arr2, Arr3: [1..10] int;
  Arr1 = 1;
  Arr2 = 2;
  Arr3 = 3;

  var Arr4 : [1..10] string;
  for i in 1..10 do 
    Arr4[i] = "str" + i:string;

  const filePath = Path.joinPath("./",
                                 "my.parquet");

  writeTable(filePath, colNames=("Arr1", "Arr2", "Arr3", "Arr4"),
             Arr1, Arr2, Arr3, Arr4);
}

UnitTest.main();
