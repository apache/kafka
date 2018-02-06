KStream<byte[], String> stream = ...;

// Java 8+ example, using lambda expressions
KStream<byte[], String> uppercased = stream.mapValues(value -> value.toUpperCase());

// Java 7 example
KStream<byte[], String> uppercased = stream.mapValues(
    new ValueMapper<String>() {
      @Override
      public String apply(String s) {
        return s.toUpperCase();
      }
    });
