package kafka.test;

import kafka.server.KafkaBroker;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;


import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(value = ClusterTestExtensions.class)
@Tag("integration")
public class AdminApiIntegrationTest {

    @ClusterTest(types = Type.KRAFT,
            serverProperties = @ClusterConfigProperty(key = "metric.reporters",
                    value = "kafka.test.AdminApiIntegrationTest$GetIdClientTelemetry"))
    public void testClientInstanceId(ClusterInstance clusterInstance) {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", clusterInstance.bootstrapServers());
        configs.put(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, "true");
        try (Admin admin = Admin.create(configs)) {
            Assertions.assertNotNull(admin.clientInstanceId(Duration.ofSeconds(3)));
        }
    }


    @ClusterTest(types = Type.KRAFT, brokers = 3)
    public void testAddRaftVoter(ClusterInstance clusterInstance) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, "true");
        String servers = clusterInstance.bootstrapServers();
        configs.put("bootstrap.servers", servers);
        try (Admin admin = Admin.create(configs)) {
            assertThrows(ExecutionException.class, () -> {
                admin.addRaftVoter(1, Uuid.randomUuid(), Collections.emptySet()).all().get();
            });
        }
    }

    @ClusterTest(types = Type.KRAFT, brokers = 3)
    public void testRemoveRaftVoter(ClusterInstance clusterInstance) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, "true");
        configs.put("bootstrap.servers", clusterInstance.bootstrapServers());
        try (Admin admin = Admin.create(configs)) {
            assertThrows(ExecutionException.class, () -> {
                admin.removeRaftVoter(1, Uuid.randomUuid()).all().get();
            });
        }
    }


    public static class GetIdClientTelemetry implements ClientTelemetry, MetricsReporter {


        @Override
        public void init(List<KafkaMetric> metrics) {
        }

        @Override
        public void metricChange(KafkaMetric metric) {
        }

        @Override
        public void metricRemoval(KafkaMetric metric) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ClientTelemetryReceiver clientReceiver() {
            return (context, payload) -> {
            };
        }
    }

}
