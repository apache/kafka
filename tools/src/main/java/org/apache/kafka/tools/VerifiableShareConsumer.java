/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.tools;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.ShareGroupAutoOffsetResetStrategy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class VerifiableShareConsumer implements Closeable, AcknowledgementCommitCallback {

    private static final Logger log = LoggerFactory.getLogger(VerifiableShareConsumer.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final PrintStream out;
    private final KafkaShareConsumer<String, String> consumer;
    private final String topic;
    private final AcknowledgementMode acknowledgementMode;
    private final String offsetResetStrategy;
    private final Boolean verbose;
    private final int maxMessages;
    private Integer totalAcknowledged = 0;
    private final String brokerHostandPort;
    private final String groupId;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public static class PartitionData {
        private final String topic;
        private final int partition;

        public PartitionData(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @JsonProperty
        public String topic() {
            return topic;
        }

        @JsonProperty
        public int partition() {
            return partition;
        }
    }

    public static class RecordSetSummary extends PartitionData {
        private final long count;
        private final Set<Long> offsets;

        public RecordSetSummary(String topic, int partition, Set<Long> offsets) {
            super(topic, partition);
            this.offsets = offsets;
            this.count = offsets.size();
        }

        @JsonProperty
        public long count() {
            return count;
        }

        @JsonProperty
        public Set<Long> offsets() {
            return offsets;
        }

    }

    protected static class AcknowledgedData extends PartitionData {
        private final long count;
        private final Set<Long> offsets;

        public AcknowledgedData(String topic, int partition, Set<Long> offsets) {
            super(topic, partition);
            this.offsets = offsets;
            this.count = offsets.size();
        }

        @JsonProperty
        public long count() {
            return count;
        }

        @JsonProperty
        public Set<Long> offsets() {
            return offsets;
        }
    }

    @JsonPropertyOrder({ "timestamp", "name" })
    private abstract static class ShareConsumerEvent {
        private final long timestamp = System.currentTimeMillis();

        @JsonProperty
        public abstract String name();

        @JsonProperty
        public long timestamp() {
            return timestamp;
        }
    }

    protected static class StartupComplete extends ShareConsumerEvent {

        @Override
        public String name() {
            return "startup_complete";
        }
    }

    @JsonPropertyOrder({ "timestamp", "name", "offsetResetStrategy" })
    protected static class OffsetResetStrategySet extends ShareConsumerEvent {

        private final String offsetResetStrategy;

        public OffsetResetStrategySet(String offsetResetStrategy) {
            this.offsetResetStrategy = offsetResetStrategy;
        }

        @Override
        public String name() {
            return "offset_reset_strategy_set";
        }

        @JsonProperty
        public String offsetResetStrategy() {
            return offsetResetStrategy;
        }
    }

    protected static class ShutdownComplete extends ShareConsumerEvent {

        @Override
        public String name() {
            return "shutdown_complete";
        }
    }

    @JsonPropertyOrder({ "timestamp", "name", "count", "partitions" })
    public static class RecordsConsumed extends ShareConsumerEvent {
        private final long count;
        private final List<RecordSetSummary> partitionSummaries;

        public RecordsConsumed(long count, List<RecordSetSummary> partitionSummaries) {
            this.count = count;
            this.partitionSummaries = partitionSummaries;
        }

        @Override
        public String name() {
            return "records_consumed";
        }

        @JsonProperty
        public long count() {
            return count;
        }

        @JsonProperty
        public List<RecordSetSummary> partitions() {
            return partitionSummaries;
        }
    }

    @JsonPropertyOrder({ "timestamp", "name", "count", "partitions", "success", "error" })
    protected static class OffsetsAcknowledged extends ShareConsumerEvent {

        private final long count;
        private final List<AcknowledgedData> partitions;
        private final String error;
        private final boolean success;

        public OffsetsAcknowledged(long count, List<AcknowledgedData> partitions, String error, boolean success) {
            this.count = count;
            this.partitions = partitions;
            this.error = error;
            this.success = success;
        }

        @Override
        public String name() {
            return "offsets_acknowledged";
        }

        @JsonProperty
        public long count() {
            return count;
        }

        @JsonProperty
        public List<AcknowledgedData> partitions() {
            return partitions;
        }

        @JsonProperty
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String error() {
            return error;
        }

        @JsonProperty
        public boolean success() {
            return success;
        }

    }

    @JsonPropertyOrder({ "timestamp", "name", "key", "value", "topic", "partition", "offset" })
    public static class RecordData extends ShareConsumerEvent {

        private final ConsumerRecord<String, String> record;

        public RecordData(ConsumerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public String name() {
            return "record_data";
        }

        @JsonProperty
        public String topic() {
            return record.topic();
        }

        @JsonProperty
        public int partition() {
            return record.partition();
        }

        @JsonProperty
        public String key() {
            return record.key();
        }

        @JsonProperty
        public String value() {
            return record.value();
        }

        @JsonProperty
        public long offset() {
            return record.offset();
        }

    }

    public VerifiableShareConsumer(KafkaShareConsumer<String, String> consumer,
                                   PrintStream out,
                                   Integer maxMessages,
                                   String topic,
                                   AcknowledgementMode acknowledgementMode,
                                   String offsetResetStrategy,
                                   String brokerHostandPort,
                                   String groupId,
                                   Boolean verbose) {
        this.out = out;
        this.consumer = consumer;
        this.topic = topic;
        this.acknowledgementMode = acknowledgementMode;
        this.offsetResetStrategy = offsetResetStrategy;
        this.verbose = verbose;
        this.maxMessages = maxMessages;
        this.brokerHostandPort = brokerHostandPort;
        this.groupId = groupId;
        addKafkaSerializerModule();
    }

    private void addKafkaSerializerModule() {
        SimpleModule kafka = new SimpleModule();
        kafka.addSerializer(TopicPartition.class, new JsonSerializer<>() {
            @Override
            public void serialize(TopicPartition tp, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                gen.writeStartObject();
                gen.writeObjectField("topic", tp.topic());
                gen.writeObjectField("partition", tp.partition());
                gen.writeEndObject();
            }
        });
        mapper.registerModule(kafka);
    }

    private void onRecordsReceived(ConsumerRecords<String, String> records) {
        List<RecordSetSummary> summaries = new ArrayList<>();
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);

            if (partitionRecords.isEmpty())
                continue;

            TreeSet<Long> partitionOffsets = new TreeSet<>();

            for (ConsumerRecord<String, String> record : partitionRecords) {
                partitionOffsets.add(record.offset());
            }

            summaries.add(new RecordSetSummary(tp.topic(), tp.partition(), partitionOffsets));

            if (this.verbose) {
                for (ConsumerRecord<String, String> record : partitionRecords) {
                    printJson(new RecordData(record));
                }
            }
        }

        printJson(new RecordsConsumed(records.count(), summaries));
    }

    @Override
    public void onComplete(Map<TopicIdPartition, Set<Long>> offsetsMap, Exception exception) {
        List<AcknowledgedData> acknowledgedOffsets = new ArrayList<>();
        int totalAcknowledged = 0;
        for (Map.Entry<TopicIdPartition, Set<Long>> offsetEntry : offsetsMap.entrySet()) {
            TopicIdPartition tp = offsetEntry.getKey();
            acknowledgedOffsets.add(new AcknowledgedData(tp.topic(), tp.partition(), offsetEntry.getValue()));
            totalAcknowledged += offsetEntry.getValue().size();
        }

        boolean success = true;
        String error = null;
        if (exception != null) {
            success = false;
            error = exception.getMessage();
        }
        printJson(new OffsetsAcknowledged(totalAcknowledged, acknowledgedOffsets, error, success));
        if (success) {
            this.totalAcknowledged += totalAcknowledged;
        }
    }

    public void run() {
        try {
            printJson(new StartupComplete());

            if (!Objects.equals(offsetResetStrategy, "")) {
                ShareGroupAutoOffsetResetStrategy offsetResetStrategy =
                    ShareGroupAutoOffsetResetStrategy.fromString(this.offsetResetStrategy);

                Properties adminClientProps = new Properties();
                adminClientProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerHostandPort);

                Admin adminClient = Admin.create(adminClientProps);

                ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, groupId);
                Map<ConfigResource, Collection<AlterConfigOp>> alterEntries = new HashMap<>();
                alterEntries.put(configResource, List.of(new AlterConfigOp(new ConfigEntry(
                    GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy.type().toString()), AlterConfigOp.OpType.SET)));
                AlterConfigsOptions alterOptions = new AlterConfigsOptions();

                // Setting the share group auto offset reset strategy
                adminClient.incrementalAlterConfigs(alterEntries, alterOptions)
                    .all()
                    .get(60, TimeUnit.SECONDS);

                printJson(new OffsetResetStrategySet(offsetResetStrategy.type().toString()));
            }

            consumer.subscribe(Collections.singleton(this.topic));
            consumer.setAcknowledgementCommitCallback(this);
            while (!(maxMessages >= 0 && totalAcknowledged >= maxMessages)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                if (!records.isEmpty()) {
                    onRecordsReceived(records);

                    if (acknowledgementMode == AcknowledgementMode.ASYNC) {
                        consumer.commitAsync();
                    } else if (acknowledgementMode == AcknowledgementMode.SYNC) {
                        Map<TopicIdPartition, Optional<KafkaException>> result = consumer.commitSync();
                        for (Map.Entry<TopicIdPartition, Optional<KafkaException>> resultEntry : result.entrySet()) {
                            if (resultEntry.getValue().isPresent()) {
                                log.error("Failed to commit offset synchronously for topic partition: {}", resultEntry.getKey());
                            }
                        }
                    }
                }
            }
        } catch (WakeupException e) {
            // ignore, we are closing
            log.trace("Caught WakeupException because share consumer is shutdown, ignore and terminate.", e);
        } catch (Throwable t) {
            // Log the error, so it goes to the service log and not stdout
            log.error("Error during processing, terminating share consumer process: ", t);
        } finally {
            consumer.close();
            printJson(new ShutdownComplete());
            shutdownLatch.countDown();
        }
    }

    public void close() {
        boolean interrupted = false;
        try {
            consumer.wakeup();
            while (true) {
                try {
                    shutdownLatch.await();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    protected synchronized void printJson(Object data) {
        try {
            out.println(mapper.writeValueAsString(data));
        } catch (JsonProcessingException e) {
            out.println("Bad data can't be written as json: " + e.getMessage());
        }
    }

    public enum AcknowledgementMode {
        AUTO, ASYNC, SYNC;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("verifiable-share-consumer")
            .defaultHelp(true)
            .description("This tool creates a share group and consumes messages from a specific topic and emits share consumer events (e.g. share consumer startup, received messages, and offsets acknowledged) as JSON objects to STDOUT.");
        MutuallyExclusiveGroup connectionGroup = parser.addMutuallyExclusiveGroup("Connection Group")
            .description("Group of arguments for connection to brokers")
            .required(true);
        connectionGroup.addArgument("--bootstrap-server")
            .action(store())
            .required(true)
            .type(String.class)
            .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
            .dest("bootstrapServer")
            .help("The server(s) to connect to. Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

        parser.addArgument("--topic")
            .action(store())
            .required(true)
            .type(String.class)
            .metavar("TOPIC")
            .help("Consumes messages from this topic.");

        parser.addArgument("--group-id")
            .action(store())
            .required(true)
            .type(String.class)
            .metavar("GROUP_ID")
            .dest("groupId")
            .help("The groupId shared among members of the share group");

        parser.addArgument("--max-messages")
            .action(store())
            .required(false)
            .type(Integer.class)
            .setDefault(-1)
            .metavar("MAX-MESSAGES")
            .dest("maxMessages")
            .help("Consume this many messages. If -1 (the default), the share consumers will consume until the process is killed externally");

        parser.addArgument("--verbose")
            .action(storeTrue())
            .type(Boolean.class)
            .metavar("VERBOSE")
            .help("Enable to log individual consumed records");

        parser.addArgument("--acknowledgement-mode")
            .action(store())
            .required(false)
            .setDefault("auto")
            .type(String.class)
            .dest("acknowledgementMode")
            .help("Acknowledgement mode for the share consumers (must be either 'auto', 'sync' or 'async')");

        parser.addArgument("--offset-reset-strategy")
            .action(store())
            .required(false)
            .setDefault("")
            .type(String.class)
            .dest("offsetResetStrategy")
            .help("Set share group reset strategy (must be either 'earliest' or 'latest')");

        parser.addArgument("--consumer.config")
            .action(store())
            .required(false)
            .type(String.class)
            .metavar("CONFIG_FILE")
            .help("Consumer config properties file (config options shared with command line parameters will be overridden).");

        return parser;
    }

    public static VerifiableShareConsumer createFromArgs(ArgumentParser parser, String[] args) throws ArgumentParserException {
        Namespace res = parser.parseArgs(args);

        AcknowledgementMode acknowledgementMode =
            AcknowledgementMode.valueOf(res.getString("acknowledgementMode").toUpperCase(Locale.ROOT));
        String offsetResetStrategy = res.getString("offsetResetStrategy").toLowerCase(Locale.ROOT);
        String configFile = res.getString("consumer.config");
        String brokerHostandPort = res.getString("bootstrapServer");

        Properties consumerProps = new Properties();
        if (configFile != null) {
            try {
                consumerProps.putAll(Utils.loadProps(configFile));
            } catch (IOException e) {
                throw new ArgumentParserException(e.getMessage(), parser);
            }
        }

        String groupId = res.getString("groupId");

        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHostandPort);

        String topic = res.getString("topic");
        int maxMessages = res.getInt("maxMessages");
        boolean verbose = res.getBoolean("verbose");

        StringDeserializer deserializer = new StringDeserializer();
        KafkaShareConsumer<String, String> consumer = new KafkaShareConsumer<>(consumerProps, deserializer, deserializer);

        return new VerifiableShareConsumer(
            consumer,
            System.out,
            maxMessages,
            topic,
            acknowledgementMode,
            offsetResetStrategy,
            brokerHostandPort,
            groupId,
            verbose);
    }

    public static void main(String[] args) {
        ArgumentParser parser = argParser();
        if (args.length == 0) {
            parser.printHelp();
            // Can't use `Exit.exit` here because it didn't exist until 0.11.0.0.
            System.exit(0);
        }
        try {
            final VerifiableShareConsumer shareConsumer = createFromArgs(parser, args);
            // Can't use `Exit.addShutdownHook` here because it didn't exist until 2.5.0.
            Runtime.getRuntime().addShutdownHook(new Thread(shareConsumer::close, "verifiable-share-consumer-shutdown-hook"));
            shareConsumer.run();
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            // Can't use `Exit.exit` here because it didn't exist until 0.11.0.0.
            System.exit(1);
        }
    }

}
