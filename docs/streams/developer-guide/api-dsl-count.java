KGroupedStream<String, Long> groupedStream = ...;
KGroupedTable<String, Long> groupedTable = ...;

// Counting a KGroupedStream
KTable<String, Long> aggregatedStream = groupedStream.count();

// Counting a KGroupedTable
KTable<String, Long> aggregatedTable = groupedTable.count();
