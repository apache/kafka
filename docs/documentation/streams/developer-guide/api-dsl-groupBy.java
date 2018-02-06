KStream<byte[], String> stream = ...;
KTable<byte[], String> table = ...;

// Java 8+ examples, using lambda expressions

// Group the stream by a new key and key type
KGroupedStream<String, String> groupedStream = stream.groupBy(
    (key, value) -> value,
    Serialized.with(
      Serdes.String(), /* key (note: type was modified) */
      Serdes.String())  /* value */
  );

// Group the table by a new key and key type, and also modify the value and value type.
KGroupedTable<String, Integer> groupedTable = table.groupBy(
    (key, value) -> KeyValue.pair(value, value.length()),
    Serialized.with(
      Serdes.String(), /* key (note: type was modified) */
      Serdes.Integer()) /* value (note: type was modified) */
  );


// Java 7 examples

// Group the stream by a new key and key type
KGroupedStream<String, String> groupedStream = stream.groupBy(
    new KeyValueMapper<byte[], String, String>>() {
      @Override
      public String apply(byte[] key, String value) {
        return value;
      }
    },
    Serialized.with(
      Serdes.String(), /* key (note: type was modified) */
      Serdes.String())  /* value */
  );

// Group the table by a new key and key type, and also modify the value and value type.
KGroupedTable<String, Integer> groupedTable = table.groupBy(
    new KeyValueMapper<byte[], String, KeyValue<String, Integer>>() {
      @Override
      public KeyValue<String, Integer> apply(byte[] key, String value) {
        return KeyValue.pair(value, value.length());
      }
    },
    Serialized.with(
      Serdes.String(), /* key (note: type was modified) */
      Serdes.Integer()) /* value (note: type was modified) */
  );
