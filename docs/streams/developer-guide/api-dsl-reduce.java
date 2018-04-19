KGroupedStream<String, Long> groupedStream = ...;
KGroupedTable<String, Long> groupedTable = ...;

// Java 8+ examples, using lambda expressions

// Reducing a KGroupedStream
KTable<String, Long> aggregatedStream = groupedStream.reduce(
    (aggValue, newValue) -> aggValue + newValue /* adder */);

// Reducing a KGroupedTable
KTable<String, Long> aggregatedTable = groupedTable.reduce(
    (aggValue, newValue) -> aggValue + newValue, /* adder */
    (aggValue, oldValue) -> aggValue - oldValue /* subtractor */);


// Java 7 examples

// Reducing a KGroupedStream
KTable<String, Long> aggregatedStream = groupedStream.reduce(
    new Reducer<Long>() { /* adder */
      @Override
      public Long apply(Long aggValue, Long newValue) {
        return aggValue + newValue;
      }
    });

// Reducing a KGroupedTable
KTable<String, Long> aggregatedTable = groupedTable.reduce(
    new Reducer<Long>() { /* adder */
      @Override
      public Long apply(Long aggValue, Long newValue) {
        return aggValue + newValue;
      }
    },
    new Reducer<Long>() { /* subtractor */
      @Override
      public Long apply(Long aggValue, Long oldValue) {
        return aggValue - oldValue;
      }
    });
