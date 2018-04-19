KStream<String, Long> left = ...;
GlobalKTable<Integer, Double> right = ...;

// Java 8+ example, using lambda expressions
KStream<String, String> joined = left.join(right,
    (leftKey, leftValue) -> leftKey.length(), /* derive a (potentially) new key by which to lookup against the table */
    (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue /* ValueJoiner */
  );

// Java 7 example
KStream<String, String> joined = left.join(right,
    new KeyValueMapper<String, Long, Integer>() { /* derive a (potentially) new key by which to lookup against the table */
      @Override
      public Integer apply(String key, Long value) {
        return key.length();
      }
    },
    new ValueJoiner<Long, Double, String>() {
      @Override
      public String apply(Long leftValue, Double rightValue) {
        return "left=" + leftValue + ", right=" + rightValue;
      }
    });
