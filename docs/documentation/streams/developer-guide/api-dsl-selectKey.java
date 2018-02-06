KStream<byte[], String> stream = ...;

// Derive a new record key from the record's value.  Note how the key type changes, too.
// Java 8+ example, using lambda expressions
KStream<String, String> rekeyed = stream.selectKey((key, value) -> value.split(" ")[0])

// Java 7 example
KStream<String, String> rekeyed = stream.selectKey(
    new KeyValueMapper<byte[], String, String>() {
      @Override
      public String apply(byte[] key, String value) {
        return value.split(" ")[0];
      }
    });
