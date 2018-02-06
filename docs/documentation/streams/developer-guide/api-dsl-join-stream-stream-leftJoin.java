import java.util.concurrent.TimeUnit;
KStream<String, Long> left = ...;
KStream<String, Double> right = ...;

// Java 8+ example, using lambda expressions
KStream<String, String> joined = left.leftJoin(right,
    (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
    JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
    Joined.with(
      Serdes.String(), /* key */
      Serdes.Long(),   /* left value */
      Serdes.Double())  /* right value */
  );

// Java 7 example
KStream<String, String> joined = left.leftJoin(right,
    new ValueJoiner<Long, Double, String>() {
      @Override
      public String apply(Long leftValue, Double rightValue) {
        return "left=" + leftValue + ", right=" + rightValue;
      }
    },
    JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
    Joined.with(
      Serdes.String(), /* key */
      Serdes.Long(),   /* left value */
      Serdes.Double())  /* right value */
  );
