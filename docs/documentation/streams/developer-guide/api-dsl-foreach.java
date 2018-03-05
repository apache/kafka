KStream<String, Long> stream = ...;

// Print the contents of the KStream to the local console.
// Java 8+ example, using lambda expressions
stream.foreach((key, value) -> System.out.println(key + " => " + value));

// Java 7 example
stream.foreach(
    new ForeachAction<String, Long>() {
      @Override
      public void apply(String key, Long value) {
        System.out.println(key + " => " + value);
      }
    });
