module TestUtil {
  import FileSystem as FS;
  import Time;

  record tempDir: contextManager {

    var path = "temp_"+Time.dateTime.now():string;

    proc ref enterContext() ref throws {
      FS.mkdir(path, parents=true);
      return this;
    }

    proc ref exitContext(in err: owned Error?) throws {
      FS.rmTree(path);
    }
  }
}
