import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;

StreamsBuilder builder = new StreamsBuilder();

GlobalKTable<String, Long> wordCounts = builder.globalTable(
    "word-counts-input-topic",
    Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(
      "word-counts-global-store" /* table/store name */)
      .withKeySerde(Serdes.String()) /* key serde */
      .withValueSerde(Serdes.Long()) /* value serde */
    );
