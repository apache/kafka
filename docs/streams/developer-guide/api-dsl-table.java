import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;

StreamsBuilder builder = new StreamsBuilder();

KTable<String, Long> wordCounts = builder.table(
  "word-counts-input-topic", /* input topic */
  Materialized.<String, Long, KeyValueStore<Bytes, byte[]>as("word-counts-partitioned-store") /* table/store name */
    .withKeySerde(Serdes.String()) /* key serde */
    .withValueSerde(Serdes.Long())   /* value serde */
  );
