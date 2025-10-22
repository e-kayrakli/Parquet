module Parquet {
  use CTypes;

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

  extern var ARROWINT64: c_int;
  extern var ARROWINT32: c_int;
  extern var ARROWUINT64: c_int;
  extern var ARROWUINT32: c_int;
  extern var ARROWBOOLEAN: c_int;
  extern var ARROWSTRING: c_int;
  extern var ARROWFLOAT: c_int;
  extern var ARROWLIST: c_int;
  extern var ARROWDOUBLE: c_int;
  extern var ARROWERROR: c_int;
  extern var ARROWDECIMAL: c_int;


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
        halt(try! "Unhandled error in extern call %s.%s (%i)".format(
                       modName, procName, lineNo));
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

  inline proc readFilesByName(ref A: [] ?t, filenames: [] string, sizes: [] int,
      dsetname: string, ty, byteLength=-1,
      hasNonFloatNulls=false) throws {
    var dummy = [false];
    readFilesByName(A, dummy, filenames, sizes, dsetname, ty, byteLength,
        hasNonFloatNulls, hasWhereNull=false);
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

  proc writeDistArrayToParquet(A, filename, dsetname, dtype, rowGroupSize,
                               compression, mode) throws {
    extern proc c_writeColumnToParquet(filename, arr_chpl, colnum,
                                       dsetname, numelems, rowGroupSize,
                                       dtype, compression, errMsg): int;
    extern proc c_appendColumnToParquet(filename, arr_chpl,
                                        dsetname, numelems,
                                        dtype, compression,
                                        errMsg): int;
    var dtypeRep = toCDtype(dtype);
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
        var locArr = A[locDom];

        numElemsPerFile[idx] = locDom.size;

        var valPtr: c_ptr(void) = nil;
        if locArr.size != 0 {
          valPtr = c_ptrTo(locArr);
        }
        if mode == TRUNCATE || !filesExist {
          writeColumn(filename, dsetname, A, dtypeRep, locDom, rowGroupSize,
                      compression);
        } else {
          manage new parquetCall(getL(), getR(), getM()) as call {
            call.retVal = c_appendColumnToParquet(myFilename.localize().c_str(),
                                                  valPtr,
                                                  dsetname.localize().c_str(),
                                                  locDom.size,
                                                  dtypeRep,
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

  proc write1DDistArrayParquet(filename: string, dsetname, dtype, compression,
                               mode, A) throws {
    return writeDistArrayToParquet(A, filename, dsetname, dtype, ROWGROUPS,
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
    var res: c_ptr(uint(8));

    manage new parquetCall(getL(), getR(), getM()) as call {
      call.retVal = c_getDatasetNames(filename.c_str(),
                                      c_ptrTo(res),
                                      false,
                                      call.errMsg);
    }
    const datasets = try! string.createAdoptingBuffer(res);

    for s in datasets.split(",") do yield s;
  }

  // TODO remove this and use the iterator everywhere, or turn this into a
  // list-returning version
  proc getDatasets(filename) throws {
    extern proc c_getDatasetNames(filename, dsetResult, readNested,
                                  errMsg): int(32);

    var res: c_ptr(uint(8));

    manage new parquetCall(getL(), getR(), getM()) as call {
      call.retVal = c_getDatasetNames(filename.c_str(),
                                      c_ptrTo(res),
                                      false,
                                      call.errMsg);
    }
    const datasets = string.createAdoptingBuffer(res);

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

  proc writeColumn(filename, colName, const ref Arr: [], dtype,
                   const ref WriteDom: domain(?) = Arr.domain,
                   rowGroupSize=ROWGROUPS,
                   compression=CompressionType.NONE) throws {
    extern proc c_writeColumnToParquet(filename, arr_chpl, colnum,
                                       dsetname, numelems, rowGroupSize,
                                       dtype, compression, errMsg): int;

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


  proc readColumn(filename, colName, ref Arr: [], ref WhereNull: [] = [0],
                  const ref ReadDom: domain(?) = Arr.domain, startIdx=0,
                  batchSize=defaultBatchSize, byteLength=-1,
                  hasNonFloatNulls=false) throws {

    extern proc c_readColumnByName(filename, arr_chpl, where_null_chpl,
                                    colName, numElems, startIdx, batchSize,
                                    byteLength, hasNonFloatNulls, errMsg): int;

    var whereNullPtr = if hasNonFloatNulls then c_ptrTo(WhereNull[ReadDom.low])
                                           else nil;

    var call = new parquetCall(getL(), getR(), getM());
    manage call {
      call.retVal = c_readColumnByName(filename=filename.localize().c_str(),
                                       arr_chpl=c_ptrTo(Arr[ReadDom.low]),
                                       where_null_chpl=whereNullPtr,
                                       colName=colName.localize().c_str(),
                                       numElems=Arr.size,
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

  private proc processParquetFilenames(filenames: [] string,
                                       matchingFilenames: [] string,
                                       mode: int) throws {
    var filesExist: bool = true;
    if mode == APPEND {
      if matchingFilenames.size == 0 {
        // Files do not exist, so we can just create the files
        filesExist = false;
      }
      else if matchingFilenames.size != filenames.size {
        throw new ParquetError("Appending to existing files must be done with "+
                               "the same number of locales. Try saving with a "+
                               "different directory or filename prefix?");
      }
    } else if mode == TRUNCATE {
      if matchingFilenames.size > 0 {
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
