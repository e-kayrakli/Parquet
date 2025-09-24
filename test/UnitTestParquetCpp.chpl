use Parquet, CTypes, FileSystem;
use UnitTest;
/*use TestBase;*/

/*require "../prereqs/cpp/obj/WriteParquet.o";*/
/*require "../prereqs/cpp/include/WriteParquet.h";*/
type c_string = c_ptrConst(c_char);

proc testReadWrite(filename: c_string, dsetname: c_string, size: int) {
  extern proc c_readColumnByName(filename, chpl_arr, whereNull, colNum,
      numElems, startIdx, batchSize, byteLength, hasNonFloatNulls, errMsg): int;
  extern proc c_writeColumnToParquet(filename, chpl_arr, colnum,
                                     dsetname, numelems, rowGroupSize, compressed,
                                     dtype, errMsg): int;
  extern proc c_free_string(a);
  extern proc strlen(a): int;
  var errMsg: c_ptr(uint(8));
  defer {
    c_free_string(errMsg);
  }
  var causeError = "cause-error":c_string;
  
  var a: [0..#size] int;
  for i in 0..#size do a[i] = i;

  if c_writeColumnToParquet(filename, c_ptrTo(a), 0, dsetname, size, 10000, false, 1, errMsg) < 0 {
    var chplMsg;
    try! chplMsg = string.createCopyingBuffer(errMsg, strlen(errMsg));
    writeln(chplMsg);
  }

  var b: [0..#size] int;

  var whereNull: [0..0] bool;
  if(c_readColumnByName(filename, c_ptrTo(b), c_ptrTo(whereNull), dsetname,
        size, 0, 10000, 1, false, c_ptrTo(errMsg)) < 0) {
    var chplMsg;
    try! chplMsg = string.createCopyingBuffer(errMsg, strlen(errMsg));
    writeln(chplMsg);
  }
    
  if a.equals(b) {
    return 0;
  } else {
    writeln("FAILED: read/write");
    return 1;
  }
}

UnitTest.main();
