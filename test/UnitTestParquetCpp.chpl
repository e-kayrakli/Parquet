use Parquet, CTypes, FileSystem;
use UnitTest;

private config const ROWGROUPS = 512*1024*1024 / numBytes(int); // 512 mb of int64

type c_string = c_ptrConst(c_char);

extern proc c_readColumnByName(filename, arr_chpl, where_null_chpl, colNum,
                               numElems, startIdx, batchSize, byteLength,
                               hasNonFloatNulls, errMsg): int;
extern proc c_writeColumnToParquet(filename, arr_chpl, colnum,
                                   dsetname, numelems, rowGroupSize,
                                   dtype, compression, errMsg): int;
extern proc c_getStringColumnNumBytes(filename, colname, offsets, numElems,
                                      startIdx, batchSize, errMsg): int;
extern proc c_getDatasetNames(f: c_string, r: c_ptr(c_ptr(c_char)),
                              readNested, errMsg): int(32);
extern proc c_writeMultiColToParquet(filename: c_string,
                                     column_names: c_ptr(void),
                                     ptr_arr: c_ptr(c_ptr(void)),
                                     offset_arr: c_ptr(c_ptr(void)),
                                     objTypes: c_ptr(void),
                                     datatypes: c_ptr(void),
                                     segArr_sizes: c_ptr(void),
                                     colnum: int,
                                     numelems: int,
                                     rowGroupSize: int,
                                     compression: int,
                                     errMsg: c_ptr(c_ptr(c_uchar))): int;

extern proc c_getNumRows(chpl_str, err): int;
extern proc c_getType(filename, colname, errMsg): c_int;

extern proc c_free_string(a);
extern proc strlen(a): int;

record parquetCall: contextManager {

  var _errMsg: c_ptr(uint(8));
  var retVal: int;

  proc ref errMsg do return c_ptrTo(_errMsg);

  proc ref enterContext() ref {
    return this;
  }

  proc ref exitContext(in err: owned Error?) throws {
    if retVal < 0 {
      var chplMsg;
      try! chplMsg = string.createCopyingBuffer(this._errMsg,
                                                strlen(this._errMsg));
      throw new Error(chplMsg);
    }
    c_free_string(_errMsg);
  }
}

proc testWriteRead(test: borrowed Test) throws {
  const filename = "myFile.parquet";
  const c_filename = filename.c_str();

  const dsetname = "myDsetname";
  const c_dsetname = dsetname.c_str();

  const size = 1000;

  var a: [0..#size] int;
  for i in 0..#size do a[i] = i;

  // create the file
  manage new parquetCall() as call do
    call.retVal = c_writeColumnToParquet(c_filename, c_ptrTo(a), 0, c_dsetname,
                                         size, 10000, false, 1, call.errMsg);

  // check getNumRows
  manage new parquetCall() as call {
    call.retVal = c_getNumRows(c_filename, call.errMsg);
    test.assertTrue(call.retVal == size);
  }

  // check getType returns something non-negative
  manage new parquetCall() as call {
    call.retVal = c_getType(c_filename, c_dsetname, call.errMsg);
  }

  // check dataset names
  manage new parquetCall() as call {
    var c_ret: c_ptr(c_char);
    defer {
      c_free_string(c_ret);
    }

    call.retVal = c_getDatasetNames(c_filename, c_ptrTo(c_ret), false, call.errMsg);

    const ret = try! string.createCopyingBuffer(c_ret, strlen(c_ret));
    test.assertTrue(ret == dsetname);
  }

  var b: [0..#size] int;
  var whereNull: [0..0] bool;
  manage new parquetCall() as call do
    call.retVal = c_readColumnByName(c_filename, c_ptrTo(b), c_ptrTo(whereNull),
                                     c_dsetname, size, 0, 10000, 1, false,
                                     call.errMsg);

  test.assertTrue(a.equals(b));
}

proc testInt32Read(test: borrowed Test) throws {
  var a: [0..#50] int;
  var expected: [0..#50] int;
  for i in 0..#50 do expected[i] = i;

  manage new parquetCall() as call do
    call.retVal = c_readColumnByName("test/resources/int32.parquet".c_str(),
                                     c_ptrTo(a), false, "array".c_str(), 50, 0,
                                     1, -1, false, call.errMsg);

  test.assertTrue(a.equals(expected));
}

proc testMultiDset(test: borrowed Test) throws {
  const filename = "test/resources/multi-col.parquet";
  const c_filename = filename.c_str();

  manage new parquetCall() as call {
    var c_ret: c_ptr(c_char);
    defer {
      c_free_string(c_ret);
    }

    call.retVal = c_getDatasetNames(c_filename, c_ptrTo(c_ret), false,
                                    call.errMsg);

    const ret = try! string.createCopyingBuffer(c_ret, strlen(c_ret));

    test.assertTrue(ret == "col1,col2,col3");
  }
}

/*
// uses lowLevelLocalizingSlice from Arkouda
/*proc testReadStrings(filename, dsetname) {*/
  /*extern proc c_readColumnByName(filename, chpl_arr, colNum, numElems, startIdx, batchSize, errMsg): int;*/
  /*extern proc c_getStringColumnNumBytes(filename, colname, offsets, numElems, startIdx, errMsg): int;*/
  /*extern proc c_getNumRows(chpl_str, err): int;*/

  /*extern proc c_free_string(a);*/
  /*extern proc strlen(a): int;*/
  /*var errMsg: c_ptr(uint(8));*/
  /*defer {*/
    /*c_free_string(errMsg);*/
  /*}*/

  /*var size = c_getNumRows(filename, c_ptrTo(errMsg));*/
  /*var offsets: [0..#size] int;*/
  
  /*c_getStringColumnNumBytes(filename, dsetname, c_ptrTo(offsets[0]), size, 0, c_ptrTo(errMsg));*/
  /*var byteSize  = + reduce offsets;*/
  /*if byteSize < 0 {*/
    /*var chplMsg;*/
    /*try! chplMsg = string.createCopyingBuffer(errMsg, strlen(errMsg));*/
    /*writeln(chplMsg);*/
  /*}*/

  /*var a: [0..#byteSize] uint(8);*/

  /*if(c_readColumnByName(filename, c_ptrTo(a), dsetname, 3, 0, 1, c_ptrTo(errMsg)) < 0) {*/
    /*var chplMsg;*/
    /*try! chplMsg = string.createCopyingBuffer(errMsg, strlen(errMsg));*/
    /*writeln(chplMsg);*/
  /*}*/

  /*var localSlice = new lowLevelLocalizingSlice(a, 0..3);*/
  /*var firstElem = string.createAdoptingBuffer(localSlice.ptr, 3, 4);*/
  /*if firstElem == 'asd' {*/
    /*return 0;*/
  /*} else {*/
    /*writeln("FAILED: reading string file ", firstElem);*/
    /*return 1;*/
  /*}*/
  
  /*return 0;*/
/*}*/
*/

UnitTest.main();
