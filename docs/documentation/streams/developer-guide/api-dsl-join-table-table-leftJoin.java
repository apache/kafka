KTable<String, Long> left = ...;
KTable<String, Double> right = ...;

// Java 8+ example, using lambda expressions
KTable<String, String> joined = left.leftJoin(right,
    (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue /* ValueJoiner */
  );

// Java 7 example
KTable<String, String> joined = left.leftJoin(right,
    new ValueJoiner<Long, Double, String>() {
      @Override
      public String apply(Long leftValue, Double rightValue) {
        return "left=" + leftValue + ", right=" + rightValue;
      }
    });
