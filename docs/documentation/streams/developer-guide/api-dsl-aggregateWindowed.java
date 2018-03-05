import java.util.concurrent.TimeUnit;
KGroupedStream<String, Long> groupedStream = ...;

// Java 8+ examples, using lambda expressions

// Aggregating with time-based windowing (here: with 5-minute tumbling windows)
KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(5)))
    .aggregate(
      () -> 0L, /* initializer */
    	(aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
      Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store") /* state store name */
        .withValueSerde(Serdes.Long())); /* serde for aggregate value */

// Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
KTable<Windowed<String>, Long> sessionizedAggregatedStream = groupedStream.windowedBy(SessionWindows.with(TimeUnit.MINUTES.toMillis(5)).
    aggregate(
    	() -> 0L, /* initializer */
    	(aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
    	(aggKey, leftAggValue, rightAggValue) -> leftAggValue + rightAggValue, /* session merger */
	    Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("sessionized-aggregated-stream-store") /* state store name */
        .withValueSerde(Serdes.Long())); /* serde for aggregate value */

// Java 7 examples

// Aggregating with time-based windowing (here: with 5-minute tumbling windows)
KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(5)))
    .aggregate(
        new Initializer<Long>() { /* initializer */
            @Override
            public Long apply() {
                return 0L;
            }
        },
        new Aggregator<String, Long, Long>() { /* adder */
            @Override
            public Long apply(String aggKey, Long newValue, Long aggValue) {
                return aggValue + newValue;
            }
        },
        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store")
          .withValueSerde(Serdes.Long()));

// Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
KTable<Windowed<String>, Long> sessionizedAggregatedStream = groupedStream.windowedBy(SessionWindows.with(TimeUnit.MINUTES.toMillis(5)).
    aggregate(
        new Initializer<Long>() { /* initializer */
            @Override
            public Long apply() {
                return 0L;
            }
        },
        new Aggregator<String, Long, Long>() { /* adder */
            @Override
            public Long apply(String aggKey, Long newValue, Long aggValue) {
                return aggValue + newValue;
            }
        },
        new Merger<String, Long>() { /* session merger */
            @Override
            public Long apply(String aggKey, Long leftAggValue, Long rightAggValue) {
                return rightAggValue + leftAggValue;
            }
        },
        Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("sessionized-aggregated-stream-store")
          .withValueSerde(Serdes.Long()));
