StreamsBuilder builder = ...;
KStream<String, Long> stream = ...;
KTable<String, Long> table = ...;

// Variant 1: Imagine that your application needs to continue reading and processing
// the records after they have been written to a topic via ``to()``.  Here, one option
// is to write to an output topic, then read from the same topic by constructing a
// new stream from it, and then begin processing it (here: via `map`, for example).
stream.to("my-stream-output-topic");
KStream<String, Long> newStream = builder.stream("my-stream-output-topic").map(...);

// Variant 2 (better): Since the above is a common pattern, the DSL provides the
// convenience method ``through`` that is equivalent to the code above.
// Note that you may need to specify key and value serdes explicitly, which is
// not shown in this simple example.
KStream<String, Long> newStream = stream.through("user-clicks-topic").map(...);

// ``through`` is also available for tables
KTable<String, Long> newTable = table.through("my-table-output-topic").map(...);
