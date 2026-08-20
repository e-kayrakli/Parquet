// Copyright Hewlett Packard Enterprise Development LP.
module TestUtil {
  import FileSystem as FS;
  import Time;

  record tempDir: contextManager {

    var path = "temp_"+Time.dateTime.now():string;

    proc ref enterContext() ref throws {
      FS.mkdir(path, parents=true);
      return this;
    }

    // Taking `err` makes this manager responsible for it, so it has to be
    // rethrown or assertion failures inside the block are silently dropped.
    proc ref exitContext(in err: owned Error?) throws {
      FS.rmTree(path);
      if err then throw err;
    }
  }
}
