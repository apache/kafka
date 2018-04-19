KStream<String, Long> left = ...;
KTable<String, Double> right = ...;

// Java 8+ example, using lambda expressions
KStream<String, String> joined = left.join(right,
    (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
    Joined.keySerde(Serdes.String()) /* key */
      .withValueSerde(Serdes.Long()) /* left value */
  );

// Java 7 example
KStream<String, String> joined = left.join(right,
    new ValueJoiner<Long, Double, String>() {
      @Override
      public String apply(Long leftValue, Double rightValue) {
        return "left=" + leftValue + ", right=" + rightValue;
      }
    },
    Joined.keySerde(Serdes.String()) /* key */
      .withValueSerde(Serdes.Long()) /* left value */
  );
