KStream<byte[], String> stream = ...;
stream.writeAsText("/path/to/local/output.txt");

// Several variants of `writeAsText` exist to e.g. override the
// default serdes for record keys and record values.
stream.writeAsText("/path/to/local/output.txt", Serdes.ByteArray(), Serdes.String());
