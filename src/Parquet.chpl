// Copyright Hewlett Packard Enterprise Development LP.
module Parquet {
  use CTypes;
  use BlockDist;

  enum CompressionType {
    NONE=0,
    SNAPPY=1,
    GZIP=2,
    BROTLI=3,
    ZSTD=4,
    LZ4=5
  };



  import Reflection.{getModuleName as getM,
                     getRoutineName as getR,
                     getLineNumber as getL};

  import List.list;
  import IO.format;
  import FileSystem as FS;
  import Path;

  extern const ARROWINT64: c_int;
  extern const ARROWINT32: c_int;
  extern const ARROWUINT64: c_int;
  extern const ARROWUINT32: c_int;
  extern const ARROWBOOLEAN: c_int;
  extern const ARROWFLOAT: c_int;
  extern const ARROWSTRING: c_int;
  extern const ARROWDOUBLE: c_int;
  extern const ARROWLIST: c_int;
  extern const ARROWDECIMAL: c_int;
  extern const ARROWERROR: c_int;

  extern const ARRAYVIEW: c_int;
  extern const PDARRAY: c_int;
  extern const STRINGS: c_int;
  extern const SEGARRAY: c_int;

  class FileWriter {
    var _wrapper : c_ptr(void);

    proc AppendRowGroup() {
      extern proc c_appendRowGroup(wrapper): c_ptr(void);

      return new RowGroupWriter(c_appendRowGroup(_wrapper));
    }

    proc close() {
      extern proc closeFileWriter(wrapper, errMsg): c_int;
      manage new parquetCall(getL(), getR(), getM()) as call {
        call.retVal = closeFileWriter(_wrapper, call.errMsg);
      }
    }
  }

  class RowGroupWriter {
    var _ptr : c_ptr(void);

    proc NextColumn() {
      extern proc c_nextColumn(ptr): c_ptr(void);

      return new ColumnWriter(c_nextColumn(_ptr));
    }
  }

  class ColumnWriter {
    var _ptr : c_ptr(void);

    proc WriteBatch(values, defLevels, repLevels, numValues) {
      extern proc c_writeBatch(ptr, values, defLevels, repLevels, numValues): c_int;

      c_writeBatch(_ptr, values, defLevels, repLevels, numValues);
    }

    proc WriteString(len, cstr, defLevels, repLevels) {
      extern proc c_writeBatchString(ptr, len, cstr, defLevels, repLevels, numValues): c_int;

      c_writeBatchString(_ptr, len, cstr, defLevels, repLevels, 1);
    }
  }

  private config const defaultBatchSize = 8192;
  config const ROWGROUPS = 512*1024*1024 / numBytes(int); // 512 mb of int64

  const TRUNCATE: int = 0;
  const APPEND: int = 1;

  class ParquetError: Error {
    proc init(msg: string) {
      super.init(msg);
    }
  }

  enum ArrowTypes { int64, int32, uint64, uint32,
                    stringArr, timestamp, boolean,
                    double, float, list, decimal,
                    notimplemented };

  proc chplTypeToCType(type t) {
    select t {
      when int(64) do return ARROWINT64;
      when int(32) do return ARROWINT32;
      when uint(64) do return ARROWUINT64;
      when uint(32) do return ARROWUINT32;
      when real do return ARROWDOUBLE;
      when bool do return ARROWBOOLEAN;
      when string do return ARROWSTRING;
      otherwise do compilerError("Unsupported Chapel type: ", t:string);
    }
  }

  record parquetCall: contextManager {
    var _errMsg: c_ptr(uint(8));
    var retVal: int;

    var err: owned Error?;

    var lineNo: int;
    var procName: string;
    var modName: string;

    proc init(lineNo, procName, modName) {
      this.lineNo = lineNo;
      this.procName = procName;
      this.modName = modName;
    }

    proc deinit() {
      // TODO errMsg is allocated through strdup in C++ code. As such, it
      // doesn't use Chapel's allocators. So, we can't really adopt the buffer
      // into a Chapel string for it causes segfaults when trying to free that
      // buffer through Chapel's allocators.
      extern proc c_free_string(ptr);
      c_free_string(_errMsg);

      // TODO this should be a thrown error in exitContext.
      // https://github.com/chapel-lang/chapel/issues/27764
      if err {
        halt(try! "Unhandled error in extern call %s.%s (%i): %s".format(
                       modName, procName, lineNo, err!.message()));
      }
    }

    proc ref errMsg do return c_ptrTo(_errMsg);

    proc ref enterContext() ref {
      return this;
    }

    proc ref exitContext(in err: owned Error?) {
      if retVal < 0 {
        var chplMsg;
        try! chplMsg = string.createCopyingBuffer(this._errMsg);
        this.err = new ParquetError(chplMsg);
      }
    }
  }

  proc getVersionInfo(): string throws {
    extern proc c_getVersionInfo(): c_ptrConst(c_char);
    extern proc c_free_string(ptr);

    const cVersionString = c_getVersionInfo();
    defer c_free_string(cVersionString: c_ptr(void));

    return string.createCopyingBuffer(cVersionString);
  }

  inline proc readFilesByName(ref A: [] ?t, filenames: [] string, sizes: [] int,
      dsetname: string, ty, byteLength=-1,
      hasNonFloatNulls=false) throws {
    var dummy = [false];
    readFilesByName(A, dummy, filenames, sizes, dsetname, ty,
      byteLength=byteLength, hasNonFloatNulls=hasNonFloatNulls,
      hasWhereNull=false);
  }

  /*
     whereNull will be populated by the CPP interface, where `true` would mean a
     0 (null) having been read.
     */
  proc readFilesByName(ref A: [] ?t, ref whereNull: [] bool,
                       filenames: [] string, sizes: [] int, dsetname: string,
                       ty, batchSize=defaultBatchSize, byteLength=-1,
                       hasNonFloatNulls=false, param hasWhereNull=true) throws {
    extern proc c_readColumnByName(filename, arr_chpl, where_null_chpl, colNum,
                                   numElems, startIdx, batchSize, byteLength,
                                   hasNonFloatNulls, errMsg): int;

    var subdoms = getSubdomains(sizes);
    var fileOffsets = (+ scan sizes) - sizes;

    coforall loc in A.targetLocales() with (ref A) do on loc {
      var locFiles = filenames;
      var locFiledoms = subdoms;
      var locOffsets = fileOffsets;

      forall (off, filedom, filename) in zip(locOffsets, locFiledoms,
                                             locFiles) {
        for locdom in A.localSubdomains() {
          const intersection = domain_intersection(locdom, filedom);
          if intersection.size > 0 {
            var whereNullPtr = if hasWhereNull
                                 then c_ptrTo(whereNull[intersection.low])
                                 else nil;

            manage new parquetCall(getL(), getR(), getM()) as call {
              call.retVal = c_readColumnByName(filename.localize().c_str(),
                                               c_ptrTo(A[intersection.low]),
                                               whereNullPtr,
                                               dsetname.localize().c_str(),
                                               intersection.size,
                                               intersection.low - off,
                                               batchSize,
                                               byteLength,
                                               hasNonFloatNulls,
                                               call.errMsg);
            }
          }
        }
      }
    }
  }

  proc readAllCols(filename: string, ref dataPtrs: [] c_ptr(void),
                   const ref types: [] c_int,
                   ref whereNullPtrs: [] c_ptr(void), numElems: int,
                   startIdx: int, batchSize=defaultBatchSize,
                   nullMode: int) throws {
    extern proc c_readAllCols(filename, chpl_arrs, types, where_null_chpl,
                             numElems, startIdx, batchSize, nullMode,
                             errMsg): c_int;

    var call = new parquetCall(getL(), getR(), getM());
    manage call {
      call.retVal = c_readAllCols(filename.localize().c_str(),
                                  c_ptrTo(dataPtrs),
                                  c_ptrToConst(types),
                                  c_ptrTo(whereNullPtrs),
                                  numElems,
                                  startIdx,
                                  batchSize,
                                  nullMode,
                                  call.errMsg);
    }
    if call.err then throw call.err;
  }

  proc readStrFilesByName(ref A: [] ?t, filenames: [] string, sizes: [] int,
                          dsetname: string, batchSize=defaultBatchSize) throws {
      extern proc c_readStrColumnByName(filename, arr_chpl, colname, numElems,
                                        batchSize, errMsg): int;

    var subdoms = getSubdomains(sizes);

    coforall loc in A.targetLocales() do on loc {
      var locFiles = filenames;
      var locFiledoms = subdoms;

      forall (filedom, filename) in zip(locFiledoms, locFiles) {
        for locdom in A.localSubdomains() {
          const intersection = domain_intersection(locdom, filedom);

          if intersection.size > 0 {
            var col: [filedom] t;

            manage new parquetCall(getL(), getR(), getM()) as call {
              call.retVal = c_readStrColumnByName(filename.localize().c_str(),
                                                  c_ptrTo(col),
                                                  dsetname.localize().c_str(),
                                                  filedom.size,
                                                  batchSize,
                                                  call.errMsg);
            }

            A[filedom] = col;
          }
        }
      }
    }
  }

  proc readListFilesByName(A: [] ?t, rows_per_file: [] int, seg_sizes: [] int,
                           offsets: [] int, filenames: [] string, sizes: [] int,
                           dsetname: string, ty) throws {
    extern proc c_readListColumnByName(filename, arr_chpl, colNum, numElems,
                                       startIdx, batchSize, errMsg): int;

    var subdoms = getSubdomains(sizes);
    var fileOffsets = (+ scan sizes) - sizes;
    var segmentOffsets = (+ scan rows_per_file) - rows_per_file;

    coforall loc in A.targetLocales() do on loc {
      var locFiles = filenames;
      var locFiledoms = subdoms;
      var locOffsets = fileOffsets; // value count offset

      // indicates which segment index is first for the file
      var locSegOffsets = segmentOffsets;

      forall (s, off, filedom, filename) in zip(locSegOffsets, locOffsets,
                                                locFiledoms, locFiles) {
        for locdom in A.localSubdomains() {
          const intersection = domain_intersection(locdom, filedom);

          if intersection.size > 0 {
            var col: [filedom] t;
            manage new parquetCall(getL(), getR(), getM()) as call {
              call.retVal = c_readListColumnByName(filename.localize().c_str(),
                                                   c_ptrTo(col),
                                                   dsetname.localize().c_str(),
                                                   filedom.size,
                                                   0,
                                                   defaultBatchSize,
                                                   call.errMsg);
            }
            A[filedom] = col;
          }
        }
      }
    }
  }

  proc calcListSizesandOffset(seg_sizes: [] ?t, filenames: [] string,
                              sizes: [] int, dsetname: string) throws {
    var subdoms = getSubdomains(sizes);

    var listSizes: [filenames.domain] int;
    var file_offset: int = 0;
    coforall loc in seg_sizes.targetLocales() with (ref listSizes) do on loc{
      var locFiles = filenames;
      var locFiledoms = subdoms;
      
      forall (i, filedom, filename) in zip(sizes.domain, locFiledoms,
                                           locFiles) {
        for locdom in seg_sizes.localSubdomains() {
          const intersection = domain_intersection(locdom, filedom);
          if intersection.size > 0 {
            var col: [filedom] t;
            listSizes[i] = getListColSize(filename, dsetname, col);
            seg_sizes[filedom] = col; // this is actually segment sizes here
          }
        }
      }
    }
    return listSizes;
  }

  proc calcStrSizesAndOffset(offsets: [] ?t, filenames: [] string,
                             sizes: [] int, dsetname: string) throws {
    const subdoms = getSubdomains(sizes);
    var byteSizes: [filenames.domain] int;

    coforall loc in offsets.targetLocales() with (ref byteSizes) do on loc {
      const locFiles = filenames;
      const locFiledoms = subdoms;

      forall (i, filedom, filename) in zip(sizes.domain, locFiledoms,
                                           locFiles) {
        for locdom in offsets.localSubdomains() {
          const intersection = domain_intersection(locdom, filedom);
          if intersection.size > 0 {
            var col: [filedom] t;
            byteSizes[i] = getStrColSize(filename, dsetname, col);
            offsets[filedom] = col;
          }
        }
      }
    }
    return byteSizes;
  }

  proc calcStrListSizesAndOffset(offsets: [] ?t, filenames: [] string,
                                 sizes: [] int, dsetname: string) throws {
    const subdoms = getSubdomains(sizes);
    var byteSizes: [filenames.domain] int;

    coforall loc in offsets.targetLocales() with (ref byteSizes) do on loc {
      const locFiles = filenames;
      const locFiledoms = subdoms;

      forall (i, filedom, filename) in zip(sizes.domain, locFiledoms,
                                           locFiles) {
        for locdom in offsets.localSubdomains() {
          const intersection = domain_intersection(locdom, filedom);
          if intersection.size > 0 {
            var col: [filedom] t;
            byteSizes[i] = getStrListColSize(filename, dsetname, col);
            offsets[filedom] = col;
          }
        }
      }
    }
    return byteSizes;
  }


  proc getNullIndices(A: [] ?t, filenames: [] string, sizes: [] int,
                      dsetname: string, ty) throws {
    extern proc c_getStringColumnNullIndices(filename, colname, nulls_chpl,
                                             errMsg): int;
    var subdoms = getSubdomains(sizes);

    coforall loc in A.targetLocales() do on loc {
      var locFiles = filenames;
      var locFiledoms = subdoms;

      forall (filedom, filename) in zip(locFiledoms, locFiles) {
        for locdom in A.localSubdomains() {
          const intersection = domain_intersection(locdom, filedom);

          if intersection.size > 0 {
            var col: [filedom] t;
            var call = new parquetCall(getL(), getR(), getM());
            manage call {
              call.retVal =
                  c_getStringColumnNullIndices(filename.localize().c_str(),
                                               dsetname.localize().c_str(),
                                               c_ptrTo(col),
                                               call.errMsg);
            }
            if call.err then throw call.err;

            A[filedom] = col;
          }
        }
      }
    }
  }

  proc getStrColSize(filename: string, dsetname: string,
                     ref offsets: [] int) throws {
    extern proc c_getStringColumnNumBytes(filename, colname, offsets, numElems,
                                          startIdx, batchSize, errMsg): int;

    var call = new parquetCall(getL(), getR(), getM());
    manage call {
      call.retVal = c_getStringColumnNumBytes(filename.localize().c_str(),
                                              dsetname.localize().c_str(),
                                              c_ptrTo(offsets),
                                              offsets.size,
                                              0,
                                              256,
                                              call.errMsg);
    }
    if call.err then throw call.err;

    return call.retVal;
  }

  proc getStrListColSize(filename: string, dsetname: string,
                         ref offsets: [] int) throws {
    extern proc c_getStringListColumnNumBytes(filename, colname, offsets,
                                              numElems, startIdx, batchSize,
                                              errMsg): int;

    var call = new parquetCall(getL(), getR(), getM());
    manage call {
      call.retVal = c_getStringListColumnNumBytes(filename.localize().c_str(),
                                                  dsetname.localize().c_str(),
                                                  c_ptrTo(offsets),
                                                  offsets.size,
                                                  0,
                                                  256,
                                                  call.errMsg);
    }
    if call.err then throw call.err;

    return call.retVal;
  }

  proc getListColSize(filename: string, dsetname: string,
                      ref seg_sizes: [] int) throws {
    extern proc c_getListColumnSize(filename, colname, seg_sizes, numElems,
                                    startIdx, errMsg): int;

    var call = new parquetCall(getL(), getR(), getM());
    manage call {
      call.retVal = c_getListColumnSize(filename.localize().c_str(),
                                        dsetname.localize().c_str(),
                                        c_ptrTo(seg_sizes),
                                        seg_sizes.size,
                                        0,
                                        call.errMsg);
    }
    if call.err then throw call.err;

    return call.retVal;
  }

  proc getArrSize(filename: string) throws {
    extern proc c_getNumRows(str_chpl, errMsg): int;

    var call = new parquetCall(getL(), getR(), getM());

    manage call {
      call.retVal = c_getNumRows(filename.localize().c_str(),
                                 call.errMsg);
    }
    if call.err then throw call.err;

    return call.retVal;
  }

  proc typeFromCType(ctype) throws {
    select ctype {
      when ARROWINT64   do return ArrowTypes.int64;
      when ARROWINT32   do return ArrowTypes.int32;
      when ARROWUINT32  do return ArrowTypes.uint32;
      when ARROWUINT64  do return ArrowTypes.uint64;
      when ARROWBOOLEAN do return ArrowTypes.boolean;
      when ARROWSTRING  do return ArrowTypes.stringArr;
      when ARROWDOUBLE  do return ArrowTypes.double;
      when ARROWFLOAT   do return ArrowTypes.float;
      when ARROWLIST    do return ArrowTypes.list;
      when ARROWDECIMAL do return ArrowTypes.decimal;
      otherwise do throw new ParquetError("Unrecognized Parquet data type");
    }
  }

  proc typeToCType(t: ArrowTypes) throws {
    select t {
      when ArrowTypes.int64     do return ARROWINT64;
      when ArrowTypes.int32     do return ARROWINT32;
      when ArrowTypes.uint64    do return ARROWUINT64;
      when ArrowTypes.uint32    do return ARROWUINT32;
      when ArrowTypes.boolean   do return ARROWBOOLEAN;
      when ArrowTypes.stringArr do return ARROWSTRING;
      when ArrowTypes.double    do return ARROWDOUBLE;
      when ArrowTypes.float     do return ARROWFLOAT;
      when ArrowTypes.list      do return ARROWLIST;
      when ArrowTypes.decimal   do return ARROWDECIMAL;
      otherwise do throw new ParquetError("Unrecognized Parquet data type");
    }
  }

  proc getArrType(filename: string, colname: string) throws {
    extern proc c_getType(filename, colname, errMsg): c_int;

    var call = new parquetCall(getL(), getR(), getM());
    manage call {
      call.retVal = c_getType(filename.localize().c_str(),
                              colname.localize().c_str(),
                              call.errMsg);
    }
    if call.err then throw call.err;

    return typeFromCType(call.retVal);
  }

  proc getListData(filename: string, dsetname: string) throws {
    extern proc c_getListType(filename, dsetname, errMsg): c_int;

    var call = new parquetCall(getL(), getR(), getM());
    manage call {
      call.retVal = c_getListType(filename.localize().c_str(),
                                  dsetname.localize().c_str(),
                                  call.errMsg);

      if call.retVal == ARROWLIST {
        throw new ParquetError("List element types cannot be list");
      }
    }
    if call.err then throw call.err;

    return typeFromCType(call.retVal);
  }

  /*
     Decimal columns in Parquet have a fixed byte length determined by their
     precision, but Parquet/Arrow doesn't expose that byte length directly.
     Since the byte length is constant for each precision value, we use a lookup
     table that maps the precision to the byte length.
  */
  proc getByteLength(filename: string, colname: string) throws {
    extern proc c_getPrecision(filename, colname, errMsg): int(32);

    var call = new parquetCall(getL(), getR(), getM());
    manage call {
      call.retVal = c_getPrecision(filename.localize().c_str(),
                                   colname.localize().c_str(),
                                   call.errMsg);
    }
    if call.err then throw call.err;

    const precision = call.retVal;
    if precision < 3 then return 1;
    else if precision < 5 then return 2;
    else if precision < 7 then return 3;
    else if precision < 10 then return 4;
    else if precision < 12 then return 5;
    else if precision < 15 then return 6;
    else if precision < 17 then return 7;
    else if precision < 19 then return 8;
    else if precision < 22 then return 9;
    else if precision < 24 then return 10;
    else if precision < 27 then return 11;
    else if precision < 29 then return 12;
    else if precision < 32 then return 13;
    else if precision < 34 then return 14;
    else if precision < 36 then return 15;
    return 16;
  }

  proc writeDistArrayToParquet(A, filename, dsetname, rowGroupSize,
                               compression, mode) throws {
    extern proc c_writeColumnToParquet(filename, arr_chpl, colnum,
                                       dsetname, numelems, rowGroupSize,
                                       dtype, compression, errMsg): int;
    extern proc c_appendColumnToParquet(filename, arr_chpl,
                                        dsetname, numelems,
                                        dtype, compression,
                                        errMsg): int;
    var (prefix, extension) = getFileMetadata(filename);

    // Generate the filenames based upon the number of targetLocales.
    var filenames = generateFilenames(prefix, extension,
                                      A.targetLocales().size);
    var numElemsPerFile: [filenames.domain] int;

    //Generate a list of matching filenames to test against. 
    var matchingFilenames = getMatchingFilenames(prefix, extension);

    var filesExist = processParquetFilenames(filenames, matchingFilenames,
                                             mode);

    if mode == APPEND {
      if filesExist {
        var datasets = getDatasets(filenames[0]);
        if datasets.contains(dsetname) then
          throw new ParquetError("A column with name " + dsetname +
                                 " already exists in Parquet file");
      }
    }

    coforall (loc, idx) in zip(A.targetLocales(), filenames.domain) do on loc {
        const myFilename = filenames[idx];

        var locDom = A.localSubdomain();
        var locArr = A[locDom]; // Engin: why are we doing this??

        numElemsPerFile[idx] = locDom.size;

        var valPtr: c_ptr(void) = nil;
        if locArr.size != 0 {
          valPtr = c_ptrTo(locArr);
        }
        if mode == TRUNCATE || !filesExist {
          writeColumn(myFilename, dsetname, A, locDom, rowGroupSize,
                      compression);
        } else {
          const dtype = chplTypeToCType(A.eltType);
          manage new parquetCall(getL(), getR(), getM()) as call {
            call.retVal = c_appendColumnToParquet(myFilename.localize().c_str(),
                                                  valPtr,
                                                  dsetname.localize().c_str(),
                                                  locDom.size,
                                                  dtype,
                                                  compression,
                                                  call.errMsg);
          }
        }
      }
    // Only warn when files are being overwritten in truncate mode
    return (filesExist && mode == TRUNCATE, filenames, numElemsPerFile);
  }

  proc createEmptyParquetFile(filename: string, dsetname: string, dtype: int,
                              compression: int) throws {
    extern proc c_createEmptyParquetFile(filename, dsetname, dtype,
                                         compression, errMsg): int;

    manage new parquetCall(getL(), getR(), getM()) as call {
      call.retVal = c_createEmptyParquetFile(filename.localize().c_str(),
                                             dsetname.localize().c_str(),
                                             dtype, compression,
                                             call.errMsg);
    }
  }

  proc writeStringsComponentToParquet(filename, dsetname,
                                      ref values: [] uint(8),
                                      ref offsets: [] int, rowGroupSize,
                                      compression, mode,
                                      filesExist) throws {
    extern proc c_writeStrColumnToParquet(filename, arr_chpl, offsets_chpl,
                                          dsetname, numelems, rowGroupSize,
                                          dtype, compression, errMsg): int;
    extern proc c_appendColumnToParquet(filename, arr_chpl,
                                        dsetname, numelems,
                                        dtype, compression,
                                        errMsg): int;

    var dtypeRep = ARROWSTRING;
    if mode == TRUNCATE || !filesExist {
      manage new parquetCall(getL(), getR(), getM()) as call {
        call.retVal = c_writeStrColumnToParquet(filename.localize().c_str(),
                                                c_ptrTo(values),
                                                c_ptrTo(offsets),
                                                dsetname.localize().c_str(),
                                                offsets.size-1,
                                                rowGroupSize,
                                                dtypeRep,
                                                compression,
                                                call.errMsg);
      }
    } else if mode == APPEND {
      manage new parquetCall(getL(), getR(), getM()) as call {
        call.retVal = c_appendColumnToParquet(filename.localize().c_str(),
                                              c_ptrTo(values),
                                              dsetname.localize().c_str(),
                                              offsets.size-1,
                                              dtypeRep,
                                              compression,
                                              call.errMsg);
      }
    }
  }

  proc writeStringsColumn(filename: string, dsetname: string,
                          const ref offsets: [] int,
                          const ref values: [] uint(8),
                          compression=CompressionType.NONE,
                          mode=TRUNCATE) throws {
    const (prefix, extension) = getFileMetadata(filename);
    const filenames = generateFilenames(prefix, extension,
                                        offsets.targetLocales().size);
    const matchingFilenames = getMatchingFilenames(prefix, extension);
    const filesExist = processParquetFilenames(filenames, matchingFilenames,
                                               mode);

    if mode == APPEND && filesExist {
      const datasets = getDatasets(filenames[0]);
      if datasets.contains(dsetname) then
        throw new ParquetError("A column with name " + dsetname +
                               " already exists in Parquet file");
    }

    coforall (loc, idx) in zip(offsets.targetLocales(), filenames.domain)
        with (const ref offsets, const ref values) do on loc {
      const myFilename = filenames[idx];
      const locDom = offsets.localSubdomain();

      if locDom.isEmpty() || locDom.size <= 0 {
        if mode == APPEND && filesExist then
          throw new ParquetError("Parquet columns must each have the same " +
                                 "length: " + myFilename);
        createEmptyParquetFile(myFilename, dsetname, ARROWSTRING,
                               compression: int);
      } else {
        const startByte = offsets[locDom.low];
        const endByte = if locDom.high == offsets.domain.high
                          then values.size
                          else offsets[locDom.high + 1];
        const numBytes = endByte - startByte;

        var localValues: [0..#numBytes] uint(8);
        if numBytes > 0 then
          localValues = values[startByte..#numBytes];

        var localOffsets: [0..#locDom.size+1] int;
        localOffsets[0..#locDom.size] = offsets[locDom] - startByte;
        localOffsets[localOffsets.domain.high] = numBytes;

        writeStringsComponentToParquet(myFilename, dsetname, localValues,
                                       localOffsets, ROWGROUPS,
                                       compression: int, mode, filesExist);
      }
    }

    return filesExist && mode == TRUNCATE;
  }

  proc write1DDistArrayParquet(filename: string, dsetname, compression,
                               mode, A) throws {
    return writeDistArrayToParquet(A, filename, dsetname, ROWGROUPS,
                                   compression, mode);
  }

  proc populateTagData(A, filenames: [?fD] string, sizes) throws {
    var subdoms = getSubdomains(sizes);
    var fileOffsets = (+ scan sizes) - sizes;

    coforall loc in A.targetLocales() do on loc {
      var locFiles = filenames;
      var locFiledoms = subdoms;
      var locOffsets = fileOffsets;

      try {
        forall (off, filedom, filename, tag) in zip(locOffsets, locFiledoms,
                                                    locFiles, 0..) {
          for locdom in A.localSubdomains() {
            const intersection = domain_intersection(locdom, filedom);

            if intersection.size > 0 {
              // write the tag into the entry
              A[intersection] = tag;
            }
          }
        }
      }
    }
  }

  iter datasets(filename) {
    extern proc c_getDatasetNames(filename, dsetResult, readNested,
                                  errMsg): int(32);
    extern proc c_free_string(ptr);
    var res: c_ptr(uint(8));

    manage new parquetCall(getL(), getR(), getM()) as call {
      call.retVal = c_getDatasetNames(filename.c_str(),
                                      c_ptrTo(res),
                                      false,
                                      call.errMsg);
    }
    defer c_free_string(res: c_ptr(void));
    const datasets = try! string.createCopyingBuffer(res);

    for s in datasets.split(",") do yield s;
  }

  // TODO remove this and use the iterator everywhere, or turn this into a
  // list-returning version
  proc getDatasets(filename, readNested=false) throws {
    extern proc c_getDatasetNames(filename, dsetResult, readNested,
                                  errMsg): int(32);
    extern proc c_free_string(ptr);

    var res: c_ptr(uint(8));

    manage new parquetCall(getL(), getR(), getM()) as call {
      call.retVal = c_getDatasetNames(filename.c_str(),
                                      c_ptrTo(res),
                                      readNested,
                                      call.errMsg);
    }
    defer c_free_string(res: c_ptr(void));
    const datasets = string.createCopyingBuffer(res);

    return new list(datasets.split(","));
  }

  proc createEmptyListParquetFile(filename: string, dsetname: string,
                                  dtype: int, compression: int) throws {
    extern proc c_createEmptyListParquetFile(filename, dsetname, dtype,
                                         compression, errMsg): int;

    manage new parquetCall(getL(), getR(), getM()) as call {
      call.retVal = c_createEmptyListParquetFile(filename.localize().c_str(),
                                                 dsetname.localize().c_str(),
                                                 dtype,
                                                 compression,
                                                 call.errMsg);
    }
  }

  /*
     Writes the local chunk of a numeric list (segarray) column for a single
     locale. `segments` gives the starting index into the values array for each
     list.
  */
  private proc writeListColumnComponent(filename: string, dsetname: string,
                                        const ref segments: [] int,
                                        const ref values: [] ?t,
                                        locDom, c_dtype, compression) throws {
    extern proc c_writeListColumnToParquet(filename, arr_chpl, offsets_chpl,
                                           dsetname, numelems, rowGroupSize,
                                           dtype, compression, errMsg): int;

    var locSegments: [0..#locDom.size+1] int;
    locSegments[0..#locDom.size] = segments[locDom];
    if locDom.high == segments.domain.high then
      locSegments[locSegments.domain.high] = values.size;
    else
      locSegments[locSegments.domain.high] = segments[locDom.high + 1];

    // Writes this locale's segments (with the given value pointer)
    // to the Parquet file.
    proc writeChunk(valPtr: c_ptr(void)) throws {
      var call = new parquetCall(getL(), getR(), getM());
      manage call {
        call.retVal = c_writeListColumnToParquet(filename.localize().c_str(),
                                                 c_ptrTo(locSegments),
                                                 valPtr,
                                                 dsetname.localize().c_str(),
                                                 locSegments.size-1,
                                                 ROWGROUPS,
                                                 c_dtype,
                                                 compression,
                                                 call.errMsg);
      }
      if call.err then throw call.err;
    }

    const valIdxRange = locSegments[0]..locSegments[locDom.size]-1;
    var localVals: [valIdxRange] t = values[valIdxRange];
    const valPtr: c_ptr(void) = if localVals.size > 0
                                  then c_ptrTo(localVals)
                                  else nil;

    writeChunk(valPtr);
  }

  /*
     Writes a numeric list (segarray) column to Parquet. `segments` is a
     distributed array where each entry is the starting index into `values` of
     the corresponding list; `values` holds the concatenated list elements.
     One file is written per target locale, matching the layout used by
     `write1DDistArrayParquet`. Returns whether existing files were overwritten.
  */
  proc writeListColumn(filename: string, colName: string,
                       const ref segments: [] int, const ref values: [] ?t,
                       compression=CompressionType.NONE) throws {
    const c_dtype = chplTypeToCType(t);
    const comp = compression: int;

    var (prefix, extension) = getFileMetadata(filename);
    var filenames = generateFilenames(prefix, extension,
                                      segments.targetLocales().size);
    var matchingFilenames = getMatchingFilenames(prefix, extension);
    var filesExist = processParquetFilenames(filenames, matchingFilenames,
                                             TRUNCATE);

    coforall (loc, idx) in zip(segments.targetLocales(), filenames.domain)
        do on loc {
      const myFilename = filenames[idx];
      const locDom = segments.localSubdomain();

      if locDom.isEmpty() || locDom.size <= 0 {
        createEmptyListParquetFile(myFilename, colName, c_dtype, comp);
      } else {
        writeListColumnComponent(myFilename, colName, segments,
                                 values, locDom, c_dtype, comp);
      }
    }

    return filesExist;
  }

  /*
     Writes the local chunk of a list-of-strings (segarray of strings) column
     for a single locale. `segments` indexes into `offsets` (one entry per
     list), `offsets` indexes into `values` (one entry per string), and
     `values` holds the raw string bytes.
  */
  private proc writeStrListColumnComponent(filename: string, dsetname: string,
                                           const ref segments: [] int,
                                           const ref offsets: [] int,
                                           const ref values: [] uint(8),
                                           locDom, dtypeRep,
                                           compression) throws {
    extern proc c_writeStrListColumnToParquet(filename, segs_chpl, offsets_chpl,
                                              arr_chpl, dsetname, numelems,
                                              rowGroupSize, dtype, compression,
                                              errMsg): int;

    // Build this locale's segment offsets with a trailing terminator so the
    // last list's length can be computed by the C writer.
    var locSegments: [0..#locDom.size+1] int;
    locSegments[0..#locDom.size] = segments[locDom];
    if locDom.high == segments.domain.high then
      locSegments[locSegments.domain.high] = offsets.size;
    else
      locSegments[locSegments.domain.high] = segments[locDom.high + 1];

    // Writes this locale's segments (with the given value/offset pointers) to
    // the Parquet file.
    proc writeChunk(offPtr: c_ptr(void), valPtr: c_ptr(void)) throws {
      var call = new parquetCall(getL(), getR(), getM());
      manage call {
        call.retVal =
            c_writeStrListColumnToParquet(filename.localize().c_str(),
                                          c_ptrTo(locSegments),
                                          offPtr,
                                          valPtr,
                                          dsetname.localize().c_str(),
                                          locSegments.size - 1,
                                          ROWGROUPS,
                                          dtypeRep,
                                          compression,
                                          call.errMsg);
      }
      if call.err then throw call.err;
    }

    // Range of string offsets owned by this locale.
    const startOffset = locSegments[0];
    const endOffset =
        if locDom.high == segments.domain.high
          then offsets.domain.high
          else segments[locDom.high + 1] - 1;
    const offsetRange = startOffset..endOffset;

    // This locale owns segments but no string bytes (all lists are empty), so
    // there are no values/offsets to send.
    if offsetRange.size <= 0 {
      writeChunk(nil, nil);
      return;
    }

    var locOffsets: [0..#offsetRange.size+1] int;
    locOffsets[0..#offsetRange.size] = offsets[offsetRange];
    locOffsets[locOffsets.domain.high] =
        if offsetRange.high == offsets.domain.high
          then values.size
          else offsets[offsetRange.high + 1];

    // Range of value bytes owned by this locale. `segments` (and thus
    // `locSegments`) index into `offsets`, and `offsets` index into `values`,
    // so the byte bounds must be read out of `offsets`, not `locSegments`.
    const startVal = offsets[offsetRange.low];
    const endVal = if offsetRange.high == offsets.domain.high
                     then values.domain.high
                     else offsets[offsetRange.high + 1] - 1;
    const valIdxRange = startVal..endVal;
    var localVals: [valIdxRange] uint(8) = values[valIdxRange];

    const offPtr: c_ptr(void) = c_ptrTo(locOffsets);
    const valPtr: c_ptr(void) = if localVals.size > 0
                                  then c_ptrTo(localVals)
                                  else nil;
    writeChunk(offPtr, valPtr);
  }

  /*
     Writes a list-of-strings (segarray of strings) column to Parquet.
     `segments` indexes into `offsets` (one entry per list), `offsets` indexes
     into `values` (one entry per string), and `values` holds the raw string
     bytes. One file is written per target locale. Returns whether existing
     files were overwritten.
  */
  proc writeStrListColumn(filename: string, colName: string,
                          const ref segments: [] int,
                          const ref offsets: [] int,
                          const ref values: [] uint(8),
                          compression=CompressionType.NONE) throws {
    const comp = compression: int;
    const dtypeRep = ARROWSTRING;

    var (prefix, extension) = getFileMetadata(filename);
    var filenames = generateFilenames(prefix, extension,
                                      segments.targetLocales().size);
    var matchingFilenames = getMatchingFilenames(prefix, extension);
    var filesExist = processParquetFilenames(filenames, matchingFilenames,
                                             TRUNCATE);

    coforall (loc, idx) in zip(segments.targetLocales(), filenames.domain)
        do on loc {
      const myFilename = filenames[idx];
      const locDom = segments.localSubdomain();

      if locDom.isEmpty() || locDom.size <= 0 {
        createEmptyListParquetFile(myFilename, colName, ARROWSTRING, comp);
      } else {
        // segment refers to segarray offsets;
        // offset refers to string byte offsets
        writeStrListColumnComponent(myFilename, colName, segments, offsets,
                                    values, locDom, dtypeRep, comp);
      }
    }
    return filesExist;
  }

  proc getNumCols(filename: string) throws {
    extern proc c_getNumCols(filename, errMsg): int(64);

    var numCols: int;
    var call = new parquetCall(getL(), getR(), getM());
    manage call {
      call.retVal = c_getNumCols(filename.c_str(), call.errMsg);
    }
    if call.err then throw call.err;
    return call.retVal;
  }

  proc getAllTypes(filename: string): [] c_int throws {
    extern proc c_getAllTypes(filename, types_out, errMsg): c_int;

    const numCols = getNumCols(filename);

    var Types: [0..#numCols] c_int;

    var call = new parquetCall(getL(), getR(), getM());
    manage call {
      call.retVal = c_getAllTypes(filename.c_str(),
                                  c_ptrTo(Types),
                                  call.errMsg);
    }
    if call.err then throw call.err;

    return Types;
  }

  proc writeColumn(filename, colName, const ref Arr: [],
                   const ref WriteDom: domain(?) = Arr.domain,
                   rowGroupSize=ROWGROUPS,
                   compression=CompressionType.NONE) throws {
    extern proc c_writeColumnToParquet(filename, arr_chpl, colnum,
                                       dsetname, numelems, rowGroupSize,
                                       dtype, compression, errMsg): int;

    const dtype = chplTypeToCType(Arr.eltType);

    var call = new parquetCall(getL(), getR(), getM());
    manage call {
      call.retVal = c_writeColumnToParquet(filename.localize().c_str(),
                                           arr_chpl=c_ptrToConst(Arr[WriteDom.low]),
                                           colnum=0,
                                           dsetname=colName.localize().c_str(),
                                           numelems=WriteDom.size,
                                           rowGroupSize=ROWGROUPS,
                                           dtype=dtype,
                                           compression=compression,
                                           call.errMsg);
    }
    if call.err then throw call.err;
  }

  record pqWriteLocalChunkInfo {
    var colName: string;
    var c_data: c_ptrConst(void);
    var c_offsets: c_ptrConst(void);
    var c_byteOffsets: c_ptrConst(void);
    var c_type: int;
    var objType: int;
    var size: int;
    var numValues: int;
    var numBytes: int;
  }

    /*
      Backing store for locale-local offsets and values registered with
      `pqWriteOp`. These arrays must outlive registration because their raw
      pointers are handed to the C++ writer at `write()` time.
    */
  class PqBuffer { }

  class Buffer : PqBuffer {
    type eltType;
    var d: domain(1);
    var data: [d] eltType;
  }

  private proc copyToBuffer(const ref values: [] ?t, start: int, count: int) {
    var buffer = new shared Buffer(t, {0..#count});
    if count > 0 then
      buffer.data = values[start..#count];
    return buffer;
  }

  record pqWriteOp {

    var filenameBase: string;
    var sharedDom: domain(?);
    var compression: int = CompressionType.NONE: int;
    var distributed = false;

    // per locale store for pqWriteLocalChunkInfo
    var info = blockDist.createArray(sharedDom.targetLocales().domain,
                                     list(pqWriteLocalChunkInfo),
                                     targetLocales=sharedDom.targetLocales());

    // per locale store keeping SegArray offset buffers alive until write()
    var buffers = blockDist.createArray(
      sharedDom.targetLocales().domain,
      list(shared PqBuffer),
      targetLocales=sharedDom.targetLocales());

    var colCount: int;

    proc ref registerColumn(const A: [] ?eltType, colName: string) {
      // TODO check domain alignment

      coforall (loc, localInfo) in zip(sharedDom.targetLocales(), info) {
        on loc {
          const ref localSubDom = A.localSubdomain();

          var ptr: c_ptrConst(void) = nil;
          if localSubDom.size > 0 then
            ptr = c_pointer_return_const(A[localSubDom.first]);

          localInfo.pushBack(
            new pqWriteLocalChunkInfo(colName.localize(),
                                      ptr,
                                      nil,
                                      nil,
                                      chplTypeToCType(eltType),
                                      PDARRAY,
                                      localSubDom.size,
                                      localSubDom.size,
                                      0));
        }
      }

      colCount += 1;
    }

    proc ref registerStrColumn(const offsets: [] int,
                               const ref values: [] uint(8),
                               colName: string) {
      coforall (loc, localInfo, localBufs) in
          zip(sharedDom.targetLocales(), info, buffers) {
        on loc {
          const ref rowDom = offsets.localSubdomain();
          const startByte = if rowDom.size > 0 then offsets[rowDom.low] else 0;
          const endByte = if rowDom.size == 0 then startByte
                          else if rowDom.high == offsets.domain.high
                            then values.size
                            else offsets[rowDom.high + 1];
          const localByteCount = endByte - startByte;

          var offsetBuf = new shared Buffer(int, {0..#rowDom.size});
          for i in 0..#rowDom.size do
            offsetBuf.data[i] = offsets[rowDom.low + i] - startByte;
          localBufs.pushBack(offsetBuf);

          const dataBuf = copyToBuffer(values, startByte, localByteCount);
          localBufs.pushBack(dataBuf);

          const offsetPtr: c_ptrConst(void) =
              if rowDom.size > 0
                then c_ptrToConst(offsetBuf.data[0]): c_ptrConst(void)
                else nil;
          const dataPtr: c_ptrConst(void) =
              if localByteCount > 0
                then c_ptrToConst(dataBuf.data[0]): c_ptrConst(void)
                else nil;

          localInfo.pushBack(
              new pqWriteLocalChunkInfo(colName.localize(),
                                        dataPtr,
                                        offsetPtr,
                                        nil,
                                        ARROWSTRING,
                                        STRINGS,
                                        rowDom.size,
                                        rowDom.size,
                                        localByteCount));
        }
      }

      colCount += 1;
    }

    /*
       Register a numeric list (SegArray) column to be written alongside other
       columns into the same Parquet file. `segments` holds one starting index
       per list into `values`, and `values` holds the concatenated list
       elements. The list column is written as a nested Arrow LIST column,
       matching the layout produced by `writeListColumn`. Supported value types
       are int(64), uint(64), real, and bool; string lists use
       `registerStrListColumn`.
    */
    proc ref registerListColumn(const segments: [] int,
                                const values: [] ?eltType,
                                colName: string) {
      if eltType != int(64) && eltType != uint(64) &&
         eltType != real && eltType != bool then
        compilerError("registerListColumn supports int(64), uint(64), real, " +
                      "and bool value types; got ", eltType:string,
                      ".");

      const c_dtype = chplTypeToCType(eltType);

      coforall (loc, localInfo, localBufs) in
          zip(sharedDom.targetLocales(), info, buffers) {
        on loc {
          const ref segDom = segments.localSubdomain();

          // Rebase this locale's segment offsets so they index from 0 into the
          // locale-local slice of `values`.
          const startVal = if segDom.size > 0 then segments[segDom.low] else 0;
          const endVal = if segDom.size == 0 then startVal
                         else if segDom.high == segments.domain.high
                           then values.size
                           else segments[segDom.high + 1];
          const numValues = endVal - startVal;
          var buf = new shared Buffer(int, {0..#segDom.size});
          for j in 0..#segDom.size do
            buf.data[j] = segments[segDom.low + j] - startVal;
          localBufs.pushBack(buf);

          var segPtr: c_ptrConst(void) = nil;
          if segDom.size > 0 then
            segPtr = c_ptrToConst(buf.data[0]): c_ptrConst(void);

          var valPtr: c_ptrConst(void) = nil;
          const dataBuf = copyToBuffer(values, startVal, numValues);
          localBufs.pushBack(dataBuf);
          if numValues > 0 then
            valPtr = c_ptrToConst(dataBuf.data[0]):c_ptrConst(void);

          localInfo.pushBack(
              new pqWriteLocalChunkInfo(colName.localize(),
                                        valPtr,
                                        segPtr,
                                        nil,
                                        c_dtype,
                                        SEGARRAY,
                                        segDom.size,
                                        numValues,
                                        0));
        }
      }

      colCount += 1;
    }

    /*
       Register a list-of-strings (SegArray of strings) column to be written
       alongside other columns into the same Parquet file. `segments` holds one
       starting index per list into `offsets`; `offsets` holds one starting
       byte index per string into `values`; and `values` holds the raw,
       null-terminated string bytes -- the same layout accepted by
       `writeStrListColumn`. The column is written as a nested Arrow LIST of
       strings.
    */
    proc ref registerStrListColumn(const segments: [] int,
                                   const offsets: [] int,
                                   const ref values: [] uint(8),
                                   colName: string) {
      coforall (loc, localInfo, localBufs) in
          zip(sharedDom.targetLocales(), info, buffers) {
        on loc {
          const ref segDom = segments.localSubdomain();

          // First and one-past-last string indices owned by this locale.
          const strStartIdx = if segDom.size > 0 then segments[segDom.low]
                                                 else 0;
          const strEndIdx = if segDom.size == 0 then 0
                            else if segDom.high == segments.domain.high
                              then offsets.size
                              else segments[segDom.high + 1];
          const numStrings = strEndIdx - strStartIdx;

          // Rebase segment offsets to index from 0 into this locale's strings.
          var segBuf = new shared Buffer(int, {0..#segDom.size});
          for j in 0..#segDom.size do
            segBuf.data[j] = segments[segDom.low + j] - strStartIdx;
          localBufs.pushBack(segBuf);

          // First and one-past-last byte indices owned by this locale.
          const byteStartIdx = if numStrings > 0 then offsets[strStartIdx]
                                                 else 0;
          const byteEndIdx = if numStrings == 0 then 0
                             else if strEndIdx == offsets.size
                               then values.size
                               else offsets[strEndIdx];
          const numByteVals = byteEndIdx - byteStartIdx;

          // Rebase byte offsets to index from 0 into this locale's bytes.
          var byteBuf = new shared Buffer(int, {0..#numStrings});
          for k in 0..#numStrings do
            byteBuf.data[k] = offsets[strStartIdx + k] - byteStartIdx;
          localBufs.pushBack(byteBuf);

          const dataBuf = copyToBuffer(values, byteStartIdx, numByteVals);
          localBufs.pushBack(dataBuf);

          var segPtr: c_ptrConst(void) = nil;
          if segDom.size > 0 then
            segPtr = c_ptrToConst(segBuf.data[0]): c_ptrConst(void);

          var byteOffPtr: c_ptrConst(void) = nil;
          if numStrings > 0 then
            byteOffPtr = c_ptrToConst(byteBuf.data[0]): c_ptrConst(void);

          var valPtr: c_ptrConst(void) = nil;
          if numByteVals > 0 then
            valPtr = c_ptrToConst(dataBuf.data[0]): c_ptrConst(void);

          localInfo.pushBack(
              new pqWriteLocalChunkInfo(colName.localize(),
                                        valPtr,
                                        segPtr,
                                        byteOffPtr,
                                        ARROWSTRING,
                                        SEGARRAY,
                                        segDom.size,
                                        numStrings,
                                        numByteVals));
        }
      }

      colCount += 1;
    }

    proc write() throws {
      extern proc createFileWriter(filename, column_names,
                                   objTypes, datatypes,
                                   colnum,
                                   compression,
                                   writer,
                                   errMsg): c_int;

      if colCount == 0 then
        throw new ParquetError("Cannot write a Parquet file with no columns");

      const useLocaleFilenames =
          distributed || sharedDom.targetLocales().size > 1;

      coforall (loc, localInfo, idx) in
          zip(sharedDom.targetLocales(), info, 0..) {
        on loc {
          assert(localInfo.size == colCount);

          const colDom = {0..#colCount};

          var localColNames: [colDom] string;
          var c_colNames: [colDom] c_ptrConst(c_char);
          var c_datas: [colDom] c_ptrConst(void);
          var c_offsets: [colDom] c_ptrConst(void);
          var c_byteOffsets: [colDom] c_ptrConst(void);
          var c_types: [colDom] int;
          var c_objTypes: [colDom] int;
          var c_numValues: [colDom] int;
          var c_numBytes: [colDom] int;
          var sizes: [colDom] int;

          for (colInfo, localColName, cColName, cData, cOffset,
               cByteOffset, cType, cObjType, cNumValue, cNumByte, size) in
              zip(localInfo, localColNames, c_colNames, c_datas, c_offsets,
                  c_byteOffsets, c_types, c_objTypes, c_numValues, c_numBytes,
                  sizes) {
            localColName = colInfo.colName;
            cColName = localColName.c_str();
            cData = colInfo.c_data;
            cOffset = colInfo.c_offsets;
            cByteOffset = colInfo.c_byteOffsets;
            cType = colInfo.c_type;
            cObjType = colInfo.objType;
            cNumValue = colInfo.numValues;
            cNumByte = colInfo.numBytes;
            size = colInfo.size;
          }

          if sizes.size > 0 && !( && reduce (sizes == sizes[0])) then
            throw new ParquetError("Parquet columns must be the same size");

          var localFilename = filenameBase.localize();
          if useLocaleFilenames {
            const (prefix, extension) = getFileMetadata(localFilename);
            localFilename = "%s_LOCALE%04i%s".format(prefix, idx, extension);
          }
          const c_filename = localFilename.c_str();
          var writer = new FileWriter();
          manage new parquetCall(getL(), getR(), getM()) as call {
            call.retVal = createFileWriter(c_filename,
                                           c_ptrTo(c_colNames),
                                           c_ptrTo(c_objTypes),
                                           c_ptrTo(c_types),
                                           colCount,
                                           compression=this.compression,
                                           c_ptrTo(writer._wrapper),
                                           call.errMsg);
          }

          const numRows = sizes[0];

          for i in 0..#numRows by ROWGROUPS {
            const batchSize = min(numRows-i, ROWGROUPS);

            var rg_writer = writer.AppendRowGroup();
            for (data, offset, byteOffset, kind, objType, numVals, numByte,
                 colRows) in
                zip(c_datas, c_offsets, c_byteOffsets, c_types, c_objTypes,
                    c_numValues, c_numBytes, sizes) {
              if objType == SEGARRAY && kind == ARROWSTRING {
                // Nested LIST-of-strings column. Each list's strings use
                // def_lvl=3, with rep_lvl=0 for the first string and rep_lvl=1
                // for the rest. Empty lists are a single null (def_lvl=1).
                // String lengths exclude the trailing null terminator.
                var col_writer = rg_writer.NextColumn();
                const segOffs = offset: c_ptrConst(int);
                const byteOffs = byteOffset: c_ptrConst(int);
                const valBytes = data: c_ptrConst(uint(8));
                for r in i..#batchSize {
                  const strStart = segOffs[r];
                  const strEnd = if r == colRows - 1 then numVals
                                                     else segOffs[r+1];
                  if strEnd - strStart > 0 {
                    for k in strStart..strEnd-1 {
                      const bStart = byteOffs[k];
                      const bEnd = if k == numVals - 1 then numByte
                                                       else byteOffs[k+1];
                      const slen = bEnd - bStart - 1;  // drop null terminator
                      const defLvl: int(16) = 3;
                      const repLvl: int(16) = (k != strStart): int(16);
                      const cstr = (valBytes + bStart): c_ptrConst(uint(8));
                      col_writer.WriteString(slen, cstr, c_ptrToConst(defLvl),
                                             c_ptrToConst(repLvl));
                    }
                  } else {
                    const defLvl: int(16) = 1;
                    const repLvl: int(16) = 0;
                    col_writer.WriteString(0, nil, c_ptrToConst(defLvl),
                                           c_ptrToConst(repLvl));
                  }
                }
              } else if objType == SEGARRAY {
                // Nested LIST column: write one list (segment) at a time using
                // Arrow definition/repetition levels. def_lvl=3 marks a defined
                // item; rep_lvl=0 starts a new list and rep_lvl=1 continues it.
                // Empty lists are written as a single null (def_lvl=1).
                var col_writer = rg_writer.NextColumn();
                const offs = offset: c_ptrConst(int);
                const elemSz = arrowElemSize(kind);
                for r in i..#batchSize {
                  const segStart = offs[r];
                  const segEnd = if r == colRows - 1 then numVals
                                                     else offs[r+1];
                  const segSize = segEnd - segStart;
                  if segSize > 0 {
                    const defLvls: [0..#segSize] int(16) = 3;
                    const repLvls = for s in 0..#segSize do (s != 0): int(16);
                    const valPtr =
                        ((data: c_ptrConst(uint(8))) +
                         segStart*elemSz): c_ptrConst(void);
                    col_writer.WriteBatch(valPtr, c_ptrToConst(defLvls),
                                          c_ptrToConst(repLvls), segSize);
                  } else {
                    const defLvl: int(16) = 1;
                    const repLvl: int(16) = 0;
                    col_writer.WriteBatch(nil, c_ptrToConst(defLvl),
                                          c_ptrToConst(repLvl), 1);
                  }
                }
              } else if objType == STRINGS && kind == ARROWSTRING {
                var col_writer = rg_writer.NextColumn();
                const byteOffs = offset: c_ptrConst(int);
                const valBytes = data: c_ptrConst(uint(8));
                for r in i..#batchSize {
                  const bStart = byteOffs[r];
                  const bEnd = if r == colRows - 1 then numByte
                                                     else byteOffs[r+1];
                  const strLen = bEnd - bStart - 1;
                  const defLvl: int(16) = 1;
                  const cstr = (valBytes + bStart): c_ptrConst(uint(8));
                  col_writer.WriteString(strLen, cstr,
                                         c_ptrToConst(defLvl), nil);
                }
              } else if kind == ARROWINT64 || kind == ARROWUINT64 ||
                        kind == ARROWBOOLEAN || kind == ARROWDOUBLE {
                var col_writer = rg_writer.NextColumn();
                const batchData =
                    ((data: c_ptrConst(uint(8))) +
                     i*arrowElemSize(kind)): c_ptrConst(void);
                col_writer.WriteBatch(batchData, nil, nil, batchSize);
              } else if kind == ARROWSTRING {
                var col_writer = rg_writer.NextColumn();
                const def_level = 1;

                var strs = data:c_ptrConst(string);
                for j in i..#batchSize {
                  const ref str = strs[j];
                  col_writer.WriteString(str.size, str.c_str(),
                                         c_ptrToConst(def_level), nil);
                }
              }
            }
          }

          writer.close();
        }
      }
    }
  }

  // Byte size of the primitive value type backing an Arrow column, used to
  // offset into a SegArray's flat value buffer during writes.
  private proc arrowElemSize(kind: int): int {
    if kind == ARROWBOOLEAN then return 1;
    return 8;
  }

  proc writeTable(filename, colNames, const Arrs...) throws {
    var op = new pqWriteOp(filename, Arrs[0].domain);

    for param i in 0..<Arrs.size do op.registerColumn(Arrs[i], colNames[i]);

    op.write();
  }

  /* This is the Chapel array-based interface */
  proc readColumn(filename, colName, ref Arr: [], ref WhereNull: [] = [0],
                  const ref ReadDom: domain(?) = Arr.domain, startIdx=0,
                  batchSize=defaultBatchSize, byteLength=-1,
                  hasNonFloatNulls=false) throws {

    var whereNullPtr = if hasNonFloatNulls then c_ptrTo(WhereNull[ReadDom.low])
                                           else nil;

    readColumn(filename=filename,
               colName=colName,
               ptr=c_ptrTo(Arr[ReadDom.low]),
               whereNullPtr=whereNullPtr,
               numElems=ReadDom.size,
               startIdx=startIdx,
               batchSize=batchSize,
               byteLength=byteLength,
               hasNonFloatNulls=hasNonFloatNulls);
  }

  /* This is the C pointer based interface */
  proc readColumn(filename, colName, ptr: c_ptr(void),
                  whereNullPtr: c_ptr(void), const numElems: int, startIdx=0,
                  batchSize=defaultBatchSize, byteLength=-1,
                  hasNonFloatNulls=false) throws {

    // TODO this should probably do dynamic type checking 
    // TODO Arr should be local

    extern proc c_readColumnByName(filename, arr_chpl, where_null_chpl,
                                    colName, numElems, startIdx, batchSize,
                                    byteLength, hasNonFloatNulls, errMsg): int;

    var call = new parquetCall(getL(), getR(), getM());
    manage call {
      call.retVal = c_readColumnByName(filename=filename.localize().c_str(),
                                       arr_chpl=ptr,
                                       where_null_chpl=whereNullPtr,
                                       colName=colName.localize().c_str(),
                                       numElems=numElems,
                                       startIdx=startIdx,
                                       batchSize=batchSize,
                                       byteLength=byteLength,
                                       hasNonFloatNulls=hasNonFloatNulls,
                                       call.errMsg);
    }
    if call.err then throw call.err;
  }

  proc toCDtype(dtype: string) throws {
    select dtype {
      when 'int64' {
        return ARROWINT64;
      } when 'uint32' {
        return ARROWUINT32;
      } when 'uint64' {
        return ARROWUINT64;
      } when 'bool' {
        return ARROWBOOLEAN;
      } when 'float64' {
        return ARROWDOUBLE;
      } when 'str' {
        return ARROWSTRING;
      } otherwise {
        throw new ParquetError("Trying to convert unrecognized dtype " +
                               "to Parquet type");
        return ARROWERROR;
      }
    }
  }

  /*
     Returns the intersection of two 1-D domains. Chapel domain slicing already
     computes the intersection for rectangular domains, so this is just a named
     wrapper around `d1[d2]`.
  */
  private proc domain_intersection(d1: domain(1), d2: domain(1)) {
    return d1[d2];
  }

  /*
     Given an array of per-file lengths, returns the contiguous index subdomain
     that each file occupies within the concatenated value space.
  */
  proc getSubdomains(lengths: [?FD] int) {
    var subdoms: [FD] domain(1);
    var offset = 0;
    for i in FD {
      subdoms[i] = {offset..#lengths[i]};
      offset += lengths[i];
    }
    return subdoms;
  }

  private proc processParquetFilenames(filenames: [] string,
                                       matchingFilenames: [] string,
                                       mode: int) throws {
    return processParquetFilenamesByCount(filenames.size,
                                         matchingFilenames.size, mode);
  }

  proc filesExistForWrite(filename: string, targetLocaleCount: int,
                          mode=TRUNCATE) throws {
    const (prefix, extension) = getFileMetadata(filename);
    const matchingFilenames = getMatchingFilenames(prefix, extension);
    return processParquetFilenamesByCount(targetLocaleCount,
                                          matchingFilenames.size, mode);
  }

  private proc processParquetFilenamesByCount(filenameCount: int,
                                              matchingFilenameCount: int,
                                              mode: int) throws {
    var filesExist: bool = true;
    if mode == APPEND {
      if matchingFilenameCount == 0 {
        // Files do not exist, so we can just create the files
        filesExist = false;
      }
      else if matchingFilenameCount != filenameCount {
        throw new ParquetError("Appending to existing files must be done with "+
                               "the same number of locales. Try saving with a "+
                               "different directory or filename prefix?");
      }
    } else if mode == TRUNCATE {
      if matchingFilenameCount > 0 {
        filesExist = true;
      } else {
        filesExist = false;
      }
    } else {
      throw new ParquetError("The mode %? is invalid".format(mode));
    }
    return filesExist;
  }

  /* Copied verbatim from Arkouda. This is a general helper in Arkouda. */
  private proc getFileMetadata(filename : string) {
    const fields = filename.split(".");
    var prefix: string;
    var extension: string;

    if fields.size == 1 || fields[fields.domain.high].count(Path.pathSep) > 0 {
      prefix = filename;
      extension = "";
    } else {
      prefix = ".".join(fields#(fields.size-1)); // take all but the last
      extension = "." + fields[fields.domain.high];
    }

    return (prefix,extension);
  }

  /* Copied verbatim from Arkouda. This is a general helper in Arkouda. */
  /*
   * Generates a list of filenames to be written to based upon a file prefix,
   * extension, and number of locales.
   */
  private proc generateFilenames(prefix : string, extension : string,
                                 targetLocalesSize:int) : [] string throws {
    /*
     * Generates a file name composed of a prefix, which is a filename provided by
     * the user along with a file index and extension.
     */
    proc generateFilename(prefix : string, extension : string,
                          idx : int) : string throws {
        var suffix = '%04i'.format(idx);
        return "%s_LOCALE%s%s".format(prefix, suffix, extension);
    }

    // Generate the filenames based upon the number of targetLocales.
    var filenames: [0..#targetLocalesSize] string;
    for i in 0..#targetLocalesSize {
      filenames[i] = generateFilename(prefix, extension, i);
    }

    return filenames;
  }

  /*
   * Generates an array of filenames to be matched in APPEND mode and to be
   * checked in TRUNCATE mode that will warn the user that 1..n files are
   * being overwritten.
   */
  private proc getMatchingFilenames(prefix : string, extension : string) throws {
      return FS.glob("%s_LOCALE*%s".format(prefix, extension));
  }

}
