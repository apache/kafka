KGroupedStream<byte[], String> groupedStream = ...;
KGroupedTable<byte[], String> groupedTable = ...;

// Java 8+ examples, using lambda expressions

// Aggregating a KGroupedStream (note how the value type changes from String to Long)
KTable<byte[], Long> aggregatedStream = groupedStream.aggregate(
    () -> 0L, /* initializer */
    (aggKey, newValue, aggValue) -> aggValue + newValue.length(), /* adder */
    Materialized.as("aggregated-stream-store") /* state store name */
        .withValueSerde(Serdes.Long()); /* serde for aggregate value */ 

// Aggregating a KGroupedTable (note how the value type changes from String to Long)
KTable<byte[], Long> aggregatedTable = groupedTable.aggregate(
    () -> 0L, /* initializer */
    (aggKey, newValue, aggValue) -> aggValue + newValue.length(), /* adder */
    (aggKey, oldValue, aggValue) -> aggValue - oldValue.length(), /* subtractor */
    Materialized.as("aggregated-table-store") /* state store name */
	.withValueSerde(Serdes.Long()) /* serde for aggregate value */


// Java 7 examples

// Aggregating a KGroupedStream (note how the value type changes from String to Long)
KTable<byte[], Long> aggregatedStream = groupedStream.aggregate(
    new Initializer<Long>() { /* initializer */
      @Override
      public Long apply() {
        return 0L;
      }
    },
    new Aggregator<byte[], String, Long>() { /* adder */
      @Override
      public Long apply(byte[] aggKey, String newValue, Long aggValue) {
        return aggValue + newValue.length();
      }
    },
    Materialized.as("aggregated-stream-store") 
        .withValueSerde(Serdes.Long()); 

// Aggregating a KGroupedTable (note how the value type changes from String to Long)
KTable<byte[], Long> aggregatedTable = groupedTable.aggregate(
    new Initializer<Long>() { /* initializer */
      @Override
      public Long apply() {
        return 0L;
      }
    },
    new Aggregator<byte[], String, Long>() { /* adder */
      @Override
      public Long apply(byte[] aggKey, String newValue, Long aggValue) {
        return aggValue + newValue.length();
      }
    },
    new Aggregator<byte[], String, Long>() { /* subtractor */
      @Override
      public Long apply(byte[] aggKey, String oldValue, Long aggValue) {
        return aggValue - oldValue.length();
      }
    },
    Materialized.as("aggregated-stream-store") 
        .withValueSerde(Serdes.Long()); 
