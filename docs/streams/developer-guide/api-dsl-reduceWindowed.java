import java.util.concurrent.TimeUnit;
KGroupedStream<String, Long> groupedStream = ...;

// Java 8+ examples, using lambda expressions

// Aggregating with time-based windowing (here: with 5-minute tumbling windows)
KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.windowedBy(
  TimeWindows.of(TimeUnit.MINUTES.toMillis(5)) /* time-based window */)
  .reduce(
    (aggValue, newValue) -> aggValue + newValue /* adder */
  );

// Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
KTable<Windowed<String>, Long> sessionzedAggregatedStream = groupedStream.windowedBy(
  SessionWindows.with(TimeUnit.MINUTES.toMillis(5))) /* session window */
  .reduce(
    (aggValue, newValue) -> aggValue + newValue /* adder */
  );


// Java 7 examples

// Aggregating with time-based windowing (here: with 5-minute tumbling windows)
KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream..windowedBy(
  TimeWindows.of(TimeUnit.MINUTES.toMillis(5)) /* time-based window */)
  .reduce(
    new Reducer<Long>() { /* adder */
      @Override
      public Long apply(Long aggValue, Long newValue) {
        return aggValue + newValue;
      }
    });

// Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.windowedBy(
  SessionWindows.with(TimeUnit.MINUTES.toMillis(5))) /* session window */
  .reduce(
    new Reducer<Long>() { /* adder */
      @Override
      public Long apply(Long aggValue, Long newValue) {
        return aggValue + newValue;
      }
    });
