import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.HashMap;
import java.util.Map;

public class ProducerApp {

    private static final String TOPIC = "commit-log";

    public static void main(String[] args) throws Exception {

        // 🔹 Read count from CLI args
        int count = 20;
        if (args.length >= 2 && args[0].equals("--count")) {
            count = Integer.parseInt(args[1]);
        }

        // 🔹 Read bootstrap servers from ENV (fallback default)
        String bootstrapServers = System.getenv()
                .getOrDefault("BOOTSTRAP_SERVERS", "primary-kafka:9092");

        // 🔹 Kafka Producer Config
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 🔹 Reliability configs (important for replication testing)
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 🔹 Idempotence (avoid duplicates)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // 🔹 Timeout configs
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();

        System.out.println("🚀 Starting Commit Log Producer...");
        System.out.println("📌 Target topic: " + TOPIC);
        System.out.println("📌 Bootstrap servers: " + bootstrapServers);
        System.out.println("📌 Event count: " + count);

        for (int i = 1; i <= count; i++) {

            // 🔹 Build JSON event
            Map<String, Object> event = new HashMap<>();
            event.put("event_id", UUID.randomUUID().toString());
            event.put("timestamp", Instant.now().getEpochSecond());
            event.put("op_type", "UPDATE");

            String key = "doc:" + UUID.randomUUID().toString().substring(0, 4);
            event.put("key", key);

            Map<String, String> value = new HashMap<>();
            value.put("status", "archived");
            event.put("value", value);

            String json = objectMapper.writeValueAsString(event);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, key, json);

            int eventNumber = i;

            // 🔹 Async send with callback
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println(
                        "✅ Event#" + eventNumber +
                        " -> partition=" + metadata.partition() +
                        ", offset=" + metadata.offset() +
                        ", key=" + key
                    );
                } else {
                    System.err.println(
                        "❌ Event#" + eventNumber +
                        " failed: " + exception.getMessage()
                    );
                }
            });

            // 🔹 Small delay (useful for observing MM2 + logs)
            Thread.sleep(50);
        }

        producer.flush();
        producer.close();

        System.out.println("🎯 Finished producing " + count + " events.");
    }
}