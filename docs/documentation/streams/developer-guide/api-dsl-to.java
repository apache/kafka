KStream<String, Long> stream = ...;
KTable<String, Long> table = ...;


// Write the stream to the output topic, using the configured default key
// and value serdes of your `StreamsConfig`.
stream.to("my-stream-output-topic");

// Same for table
table.to("my-table-output-topic");

// Write the stream to the output topic, using explicit key and value serdes,
// (thus overriding the defaults of your `StreamsConfig`).
stream.to("my-stream-output-topic", Produced.with(Serdes.String(), Serdes.Long());
