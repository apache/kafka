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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.server.util.ShutdownableThread;
import org.apache.kafka.tools.filter.TopicFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import joptsimple.OptionSpec;

import static java.lang.String.format;

/**
 * For verifying the consistency among replicas.
 * <p>
 *  1. start a fetcher on every broker
 *  2. each fetcher does the following
 *    2.1 issues fetch request
 *    2.2 puts the fetched result in a shared buffer
 *    2.3 waits for all other fetchers to finish step 2.2
 *    2.4 one of the fetchers verifies the consistency of fetched results among replicas
 * <p>
 * The consistency verification is up to the high watermark. The tool reports the
 * max lag between the verified offset and the high watermark among all partitions.
 * <p>
 * If a broker goes down, the verification of the partitions on that broker is delayed
 * until the broker is up again.
 * <p>
 * Caveats:
 * 1. The tool needs all brokers to be up at startup time.
 * 2. The tool doesn't handle out of range offsets.
 */
public class ReplicaVerificationTool {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaVerificationTool.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    public static void main(String[] args) {
        try {
            LOG.warn("This tool is deprecated and may be removed in a future major release.");

            ReplicaVerificationToolOptions options = new ReplicaVerificationToolOptions(args);
            // getting topic metadata
            LOG.info("Getting topic metadata...");
            String brokerList = options.brokerHostsAndPorts();

            try (Admin adminClient = createAdminClient(brokerList)) {
                Collection<TopicDescription> topicsMetadata = listTopicsMetadata(adminClient);
                Map<Integer, Node> brokerInfo = brokerDetails(adminClient);

                Map<String, Uuid> topicIds = topicsMetadata.stream().collect(Collectors.toMap(TopicDescription::name, TopicDescription::topicId));

                List<TopicDescription> filteredTopicMetadata = topicsMetadata.stream().filter(
                    topicMetadata -> options.topicsIncludeFilter().isTopicAllowed(topicMetadata.name(), false)
                ).collect(Collectors.toList());

                if (filteredTopicMetadata.isEmpty()) {
                    LOG.error("No topics found. {} if specified, is either filtering out all topics or there is no topic.", options.topicsIncludeOpt);
                    Exit.exit(1);
                }

                List<TopicPartitionReplica> topicPartitionReplicas = filteredTopicMetadata.stream().flatMap(
                    topicMetadata -> topicMetadata.partitions().stream().flatMap(
                        partitionMetadata -> partitionMetadata.replicas().stream().map(
                            node -> new TopicPartitionReplica(topicMetadata.name(), partitionMetadata.partition(), node.id())
                        )
                    )
                ).collect(Collectors.toList());
                LOG.debug("Selected topic partitions: {}", topicPartitionReplicas);

                Map<Integer, List<TopicPartition>> brokerToTopicPartitions = topicPartitionReplicas.stream()
                    .collect(Collectors.groupingBy(
                        TopicPartitionReplica::brokerId,
                        Collectors.mapping(
                            replica -> new TopicPartition(replica.topic(), replica.partition()),
                            Collectors.toList()
                        )
                    ));
                LOG.debug("Topic partitions per broker: {}", brokerToTopicPartitions);

                Map<TopicPartition, Integer> expectedReplicasPerTopicPartition = topicPartitionReplicas.stream()
                    .collect(Collectors.groupingBy(
                        replica -> new TopicPartition(replica.topic(), replica.partition()),
                        Collectors.collectingAndThen(
                            Collectors.toList(),
                            List::size
                        )
                    ));
                LOG.debug("Expected replicas per topic partition: {}", expectedReplicasPerTopicPartition);

                List<TopicPartition> topicPartitions = filteredTopicMetadata.stream()
                    .flatMap(topicMetadata -> topicMetadata.partitions().stream()
                        .map(partitionMetadata -> new TopicPartition(topicMetadata.name(), partitionMetadata.partition()))
                    )
                    .collect(Collectors.toList());

                Properties consumerProps = consumerConfig(brokerList);

                ReplicaBuffer replicaBuffer = new ReplicaBuffer(expectedReplicasPerTopicPartition,
                    initialOffsets(topicPartitions, consumerProps, options.initialOffsetTime()),
                    brokerToTopicPartitions.size(), options.reportInterval());

                // create all replica fetcher threads
                int verificationBrokerId = brokerToTopicPartitions.entrySet().iterator().next().getKey();
                AtomicInteger counter = new AtomicInteger(0);
                List<ReplicaFetcher> fetcherThreads = brokerToTopicPartitions.entrySet().stream()
                    .map(entry -> {
                        int brokerId = entry.getKey();
                        Iterable<TopicPartition> partitions = entry.getValue();
                        return new ReplicaFetcher(
                            "ReplicaFetcher-" + brokerId,
                            brokerInfo.get(brokerId),
                            partitions,
                            topicIds,
                            replicaBuffer,
                            options.fetchSize(),
                            options.maxWaitMs(),
                            1,
                            brokerId == verificationBrokerId,
                            consumerProps,
                            counter.incrementAndGet()
                        );
                    })
                    .collect(Collectors.toList());

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    LOG.info("Stopping all fetchers");
                    fetcherThreads.forEach(replicaFetcher -> {
                        try {
                            replicaFetcher.shutdown();
                        } catch (InterruptedException ignored) {
                        }
                    });
                }, "ReplicaVerificationToolShutdownHook"));

                fetcherThreads.forEach(Thread::start);
                System.out.printf("%s: verification process is started%n",
                    DATE_FORMAT.format(new Date(Time.SYSTEM.milliseconds())));
            }
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            Exit.exit(1);
        }
    }

    private static Map<TopicPartition, Long> initialOffsets(List<TopicPartition> topicPartitions, Properties consumerConfig, long initialOffsetTime) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            if (ListOffsetsRequest.LATEST_TIMESTAMP == initialOffsetTime) {
                return consumer.endOffsets(topicPartitions).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            } else if (ListOffsetsRequest.EARLIEST_TIMESTAMP == initialOffsetTime) {
                return consumer.beginningOffsets(topicPartitions).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            } else {
                Map<TopicPartition, Long> timestampsToSearch = topicPartitions.stream()
                    .collect(Collectors.toMap(Function.identity(), tp -> initialOffsetTime));
                return consumer.offsetsForTimes(timestampsToSearch).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
            }
        }
    }

    private static Properties consumerConfig(String brokerUrl) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ReplicaVerification");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    private static Map<Integer, Node> brokerDetails(Admin adminClient) throws ExecutionException, InterruptedException {
        return adminClient.describeCluster().nodes().get().stream().collect(Collectors.toMap(Node::id, Function.identity()));
    }

    private static Collection<TopicDescription> listTopicsMetadata(Admin adminClient) throws ExecutionException, InterruptedException {
        Set<String> topics = adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
        return adminClient.describeTopics(topics).allTopicNames().get().values();
    }

    private static Admin createAdminClient(String brokerList) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return Admin.create(props);
    }

    private static class ReplicaVerificationToolOptions extends CommandDefaultOptions {
        private final OptionSpec<String> brokerListOpt;
        private final OptionSpec<Integer> fetchSizeOpt;
        private final OptionSpec<Integer> maxWaitMsOpt;
        private final OptionSpec<String> topicsIncludeOpt;
        private final OptionSpec<Long> initialOffsetTimeOpt;
        private final OptionSpec<Long> reportIntervalOpt;

        ReplicaVerificationToolOptions(String[] args) {
            super(args);
            brokerListOpt = parser.accepts("broker-list", "REQUIRED: The list of hostname and port of the server to connect to.")
                .withRequiredArg()
                .describedAs("hostname:port,...,hostname:port")
                .ofType(String.class);
            fetchSizeOpt = parser.accepts("fetch-size", "The fetch size of each request.")
                .withRequiredArg()
                .describedAs("bytes")
                .ofType(Integer.class)
                .defaultsTo(ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES);
            maxWaitMsOpt = parser.accepts("max-wait-ms", "The max amount of time each fetch request waits.")
                .withRequiredArg()
                .describedAs("ms")
                .ofType(Integer.class)
                .defaultsTo(1_000);
            topicsIncludeOpt = parser.accepts("topics-include", "List of topics to verify replica consistency.")
                .withRequiredArg()
                .describedAs("Java regex (String)")
                .ofType(String.class)
                .defaultsTo(".*");
            initialOffsetTimeOpt = parser.accepts("time", "Timestamp for getting the initial offsets.")
                .withRequiredArg()
                .describedAs("timestamp/-1(latest)/-2(earliest)")
                .ofType(Long.class)
                .defaultsTo(-1L);
            reportIntervalOpt = parser.accepts("report-interval-ms", "The reporting interval.")
                .withRequiredArg()
                .describedAs("ms")
                .ofType(Long.class)
                .defaultsTo(30_000L);
            options = parser.parse(args);
            if (args.length == 0 || options.has(helpOpt)) {
                CommandLineUtils.printUsageAndExit(parser, "Validate that all replicas for a set of topics have the same data.");
            }
            if (options.has(versionOpt)) {
                CommandLineUtils.printVersionAndExit();
            }
            CommandLineUtils.checkRequiredArgs(parser, options, brokerListOpt);
        }

        String brokerHostsAndPorts() {
            String brokerList = options.valueOf(brokerListOpt);

            try {
                ToolsUtils.validateBootstrapServer(brokerList);
            } catch (IllegalArgumentException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
            }

            return brokerList;
        }

        TopicFilter.IncludeList topicsIncludeFilter() {
            String regex = options.valueOf(topicsIncludeOpt);
            try {
                Pattern.compile(regex);
            } catch (PatternSyntaxException e) {
                throw new RuntimeException(format("%s is an invalid regex", regex));
            }
            return new TopicFilter.IncludeList(regex);
        }

        int fetchSize() {
            return options.valueOf(fetchSizeOpt);
        }

        int maxWaitMs() {
            return options.valueOf(maxWaitMsOpt);
        }

        long initialOffsetTime() {
            return options.valueOf(initialOffsetTimeOpt);
        }

        long reportInterval() {
            return options.valueOf(reportIntervalOpt);
        }
    }

    private static class MessageInfo {
        final int replicaId;
        final long offset;
        final long nextOffset;
        final long checksum;

        MessageInfo(int replicaId, long offset, long nextOffset, long checksum) {
            this.replicaId = replicaId;
            this.offset = offset;
            this.nextOffset = nextOffset;
            this.checksum = checksum;
        }
    }

    protected static class ReplicaBuffer {
        private final Map<TopicPartition, Integer> expectedReplicasPerTopicPartition;
        private final int expectedNumFetchers;
        private final long reportInterval;
        private final Map<TopicPartition, Long> fetchOffsetMap;
        private final Map<TopicPartition, Map<Integer, FetchResponseData.PartitionData>> recordsCache;
        private final AtomicReference<CountDownLatch> fetcherBarrier;
        private final AtomicReference<CountDownLatch> verificationBarrier;

        private volatile long lastReportTime;
        private long maxLag;
        private long offsetWithMaxLag;
        private TopicPartition maxLagTopicAndPartition;

        ReplicaBuffer(Map<TopicPartition, Integer> expectedReplicasPerTopicPartition,
                      Map<TopicPartition, Long> initialOffsets,
                      int expectedNumFetchers,
                      long reportInterval) {
            this.expectedReplicasPerTopicPartition = expectedReplicasPerTopicPartition;
            this.expectedNumFetchers = expectedNumFetchers;
            this.reportInterval = reportInterval;
            this.fetchOffsetMap = new HashMap<>();
            this.recordsCache = new HashMap<>();
            this.fetcherBarrier = new AtomicReference<>(new CountDownLatch(expectedNumFetchers));
            this.verificationBarrier = new AtomicReference<>(new CountDownLatch(1));
            this.lastReportTime = Time.SYSTEM.milliseconds();
            this.maxLag = -1L;
            this.offsetWithMaxLag = -1L;

            for (TopicPartition topicPartition : expectedReplicasPerTopicPartition.keySet()) {
                recordsCache.put(topicPartition, new HashMap<>());
            }
            // set initial offsets
            for (Map.Entry<TopicPartition, Long> entry : initialOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                Long offset = entry.getValue();
                fetchOffsetMap.put(tp, offset);
            }
        }

        void createNewFetcherBarrier() {
            fetcherBarrier.set(new CountDownLatch(expectedNumFetchers));
        }

        CountDownLatch getFetcherBarrier() {
            return fetcherBarrier.get();
        }

        void createNewVerificationBarrier() {
            verificationBarrier.set(new CountDownLatch(1));
        }

        CountDownLatch getVerificationBarrier() {
            return verificationBarrier.get();
        }

        void addFetchedData(TopicPartition topicPartition,
                            int replicaId,
                            FetchResponseData.PartitionData partitionData) {
            recordsCache.get(topicPartition).put(replicaId, partitionData);
        }

        long getOffset(TopicPartition topicPartition) {
            return fetchOffsetMap.get(topicPartition);
        }

        void verifyCheckSum(Consumer<String> println) {
            LOG.debug("Begin verification");
            maxLag = -1L;

            for (Map.Entry<TopicPartition, Map<Integer, FetchResponseData.PartitionData>> cacheEntry : recordsCache.entrySet()) {
                TopicPartition topicPartition = cacheEntry.getKey();
                Map<Integer, FetchResponseData.PartitionData> fetchResponsePerReplica = cacheEntry.getValue();

                LOG.debug("Verifying {}", topicPartition);
                assert fetchResponsePerReplica.size() == expectedReplicasPerTopicPartition.get(topicPartition) :
                    "fetched " + fetchResponsePerReplica.size() + " replicas for " + topicPartition +
                        ", but expected " + expectedReplicasPerTopicPartition.get(topicPartition) + " replicas";

                Map<Integer, Iterator<? extends RecordBatch>> recordBatchIteratorMap = new HashMap<>();
                for (Map.Entry<Integer, FetchResponseData.PartitionData> fetchResEntry : fetchResponsePerReplica.entrySet()) {
                    int replicaId = fetchResEntry.getKey();
                    FetchResponseData.PartitionData fetchResponse = fetchResEntry.getValue();
                    Iterator<? extends RecordBatch> recordIterator =
                        FetchResponse.recordsOrFail(fetchResponse).batches().iterator();
                    recordBatchIteratorMap.put(replicaId, recordIterator);
                }

                long maxHw = fetchResponsePerReplica.values().stream()
                    .mapToLong(FetchResponseData.PartitionData::highWatermark)
                    .max().orElse(-1L);

                boolean isMessageInAllReplicas = true;

                // iterate one message at a time from every replica, until high watermark is reached
                while (isMessageInAllReplicas) {
                    Optional<MessageInfo> messageInfoFromFirstReplicaOpt = Optional.empty();

                    for (Map.Entry<Integer, Iterator<? extends RecordBatch>> batchEntry : recordBatchIteratorMap.entrySet()) {
                        int replicaId = batchEntry.getKey();
                        Iterator<? extends RecordBatch> recordBatchIterator = batchEntry.getValue();

                        try {
                            if (recordBatchIterator.hasNext()) {
                                RecordBatch batch = recordBatchIterator.next();

                                // only verify up to the high watermark
                                if (batch.lastOffset() >= fetchResponsePerReplica.get(replicaId).highWatermark()) {
                                    isMessageInAllReplicas = false;
                                } else {
                                    if (!messageInfoFromFirstReplicaOpt.isPresent()) {
                                        messageInfoFromFirstReplicaOpt = Optional.of(
                                            new MessageInfo(replicaId, batch.lastOffset(), batch.nextOffset(), batch.checksum())
                                        );
                                    } else {
                                        MessageInfo messageInfoFromFirstReplica = messageInfoFromFirstReplicaOpt.get();

                                        if (messageInfoFromFirstReplica.offset != batch.lastOffset()) {
                                            println.accept(DATE_FORMAT.format(new Date(Time.SYSTEM.milliseconds())) +
                                                ": partition " + topicPartition +
                                                ": replica " + messageInfoFromFirstReplica.replicaId +
                                                "'s offset " + messageInfoFromFirstReplica.offset +
                                                " doesn't match replica " + replicaId +
                                                "'s offset " + batch.lastOffset());
                                            Exit.exit(1);
                                        }

                                        if (messageInfoFromFirstReplica.checksum != batch.checksum())
                                            println.accept(DATE_FORMAT.format(new Date(Time.SYSTEM.milliseconds())) +
                                                ": partition " + topicPartition +
                                                " has unmatched checksum at offset " + batch.lastOffset() +
                                                "; replica " + messageInfoFromFirstReplica.replicaId +
                                                "'s checksum " + messageInfoFromFirstReplica.checksum +
                                                "; replica " + replicaId + "'s checksum " + batch.checksum());
                                    }
                                }
                            } else {
                                isMessageInAllReplicas = false;
                            }
                        } catch (Throwable t) {
                            throw new RuntimeException("Error in processing replica " + replicaId +
                                " in partition " + topicPartition + " at offset " +
                                fetchOffsetMap.get(topicPartition), t);
                        }
                    }

                    if (isMessageInAllReplicas) {
                        long nextOffset = messageInfoFromFirstReplicaOpt.map(messageInfo -> messageInfo.nextOffset).orElse(-1L);
                        fetchOffsetMap.put(topicPartition, nextOffset);
                        LOG.debug("{} replicas match at offset {} for {}",
                            expectedReplicasPerTopicPartition.get(topicPartition), nextOffset, topicPartition);
                    }
                }

                if (maxHw - fetchOffsetMap.get(topicPartition) > maxLag) {
                    offsetWithMaxLag = fetchOffsetMap.get(topicPartition);
                    maxLag = maxHw - offsetWithMaxLag;
                    maxLagTopicAndPartition = topicPartition;
                }

                fetchResponsePerReplica.clear();
            }

            long currentTimeMs = Time.SYSTEM.milliseconds();
            if (currentTimeMs - lastReportTime > reportInterval) {
                println.accept(DATE_FORMAT.format(new Date(currentTimeMs)) +
                    ": max lag is " + maxLag + " for partition " +
                    maxLagTopicAndPartition + " at offset " + offsetWithMaxLag +
                    " among " + recordsCache.size() + " partitions");
                lastReportTime = currentTimeMs;
            }
        }
    }

    private static class ReplicaFetcher extends ShutdownableThread {
        private final Node sourceBroker;
        private final Iterable<TopicPartition> topicPartitions;
        private final Map<String, Uuid> topicIds;
        private final ReplicaBuffer replicaBuffer;
        private final int fetchSize;
        private final int maxWait;
        private final int minBytes;
        private final boolean doVerification;
        private final ReplicaFetcherBlockingSend fetchEndpoint;
        private final Map<Uuid, String> topicNames;

        public ReplicaFetcher(String name,
                              Node sourceBroker,
                              Iterable<TopicPartition> topicPartitions,
                              Map<String, Uuid> topicIds,
                              ReplicaBuffer replicaBuffer,
                              int fetchSize,
                              int maxWait,
                              int minBytes,
                              boolean doVerification,
                              Properties consumerConfig,
                              int fetcherId) {
            super(name);
            this.sourceBroker = sourceBroker;
            this.topicPartitions = topicPartitions;
            this.topicIds = topicIds;
            this.replicaBuffer = replicaBuffer;
            this.fetchSize = fetchSize;
            this.maxWait = maxWait;
            this.minBytes = minBytes;
            this.doVerification = doVerification;
            this.fetchEndpoint = new ReplicaFetcherBlockingSend(sourceBroker, new ConsumerConfig(consumerConfig), new Metrics(),
                Time.SYSTEM, fetcherId, "broker-" + FetchRequest.DEBUGGING_CONSUMER_ID + "-fetcher-" + fetcherId);
            this.topicNames = topicIds.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        }

        @Override
        public void doWork() {
            CountDownLatch fetcherBarrier = replicaBuffer.getFetcherBarrier();
            CountDownLatch verificationBarrier = replicaBuffer.getVerificationBarrier();

            Map<TopicPartition, FetchRequest.PartitionData> requestMap = new LinkedHashMap<>();
            for (TopicPartition topicPartition : topicPartitions) {
                requestMap.put(topicPartition, new FetchRequest.PartitionData(
                    topicIds.getOrDefault(topicPartition.topic(), Uuid.ZERO_UUID),
                    replicaBuffer.getOffset(topicPartition),
                    0L,
                    fetchSize,
                    Optional.empty()
                ));
            }

            FetchRequest.Builder fetchRequestBuilder = FetchRequest.Builder.forReplica(
                ApiKeys.FETCH.latestVersion(),
                FetchRequest.DEBUGGING_CONSUMER_ID,
                -1,
                maxWait,
                minBytes,
                requestMap
            );

            LOG.debug("Issuing fetch request");

            FetchResponse fetchResponse = null;
            try {
                ClientResponse clientResponse = fetchEndpoint.sendRequest(fetchRequestBuilder);
                fetchResponse = (FetchResponse) clientResponse.responseBody();
            } catch (Throwable t) {
                if (!isRunning())
                    throw new RuntimeException(t);
            }

            if (fetchResponse != null) {
                fetchResponse.responseData(topicNames, ApiKeys.FETCH.latestVersion()).forEach((tp, partitionData) ->
                    replicaBuffer.addFetchedData(tp, sourceBroker.id(), partitionData));
            } else {
                for (TopicPartition topicAndPartition : topicPartitions) {
                    replicaBuffer.addFetchedData(
                        topicAndPartition,
                        sourceBroker.id(),
                        FetchResponse.partitionResponse(topicAndPartition.partition(), Errors.NONE)
                    );
                }
            }

            fetcherBarrier.countDown();
            LOG.debug("Done fetching");

            // wait for all fetchers to finish
            try {
                fetcherBarrier.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            LOG.debug("Ready for verification");

            // one of the fetchers will do the verification
            if (doVerification) {
                LOG.debug("Do verification");
                replicaBuffer.verifyCheckSum(System.out::println);
                replicaBuffer.createNewFetcherBarrier();
                replicaBuffer.createNewVerificationBarrier();
                LOG.debug("Created new barrier");
                verificationBarrier.countDown();
            }

            try {
                verificationBarrier.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            LOG.debug("Done verification");
        }
    }

    private static class ReplicaFetcherBlockingSend {
        private final Node sourceNode;
        private final Time time;
        private final int socketTimeout;
        private final NetworkClient networkClient;

        ReplicaFetcherBlockingSend(Node sourceNode,
                                   ConsumerConfig consumerConfig,
                                   Metrics metrics,
                                   Time time,
                                   int fetcherId,
                                   String clientId) {
            this.sourceNode = sourceNode;
            this.time = time;
            this.socketTimeout = consumerConfig.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);

            LogContext logContext = new LogContext();
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(consumerConfig, time, logContext);
            Selector selector = new Selector(
                NetworkReceive.UNLIMITED,
                consumerConfig.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                metrics,
                time,
                "replica-fetcher",
                new HashMap<String, String>() {{
                        put("broker-id", sourceNode.idString());
                        put("fetcher-id", String.valueOf(fetcherId));
                    }},
                false,
                channelBuilder,
                logContext
            );
            this.networkClient = new NetworkClient(
                selector,
                new ManualMetadataUpdater(),
                clientId,
                1,
                0,
                0,
                Selectable.USE_DEFAULT_BUFFER_SIZE,
                consumerConfig.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
                consumerConfig.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                consumerConfig.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                consumerConfig.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                time,
                false,
                new ApiVersions(),
                logContext,
                MetadataRecoveryStrategy.forName(consumerConfig.getString(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG))
            );
        }

        ClientResponse sendRequest(AbstractRequest.Builder<? extends AbstractRequest> requestBuilder) {
            try {
                if (!NetworkClientUtils.awaitReady(networkClient, sourceNode, time, socketTimeout))
                    throw new SocketTimeoutException("Failed to connect within " + socketTimeout + " ms");
                else {
                    ClientRequest clientRequest = networkClient.newClientRequest(sourceNode.idString(),
                        requestBuilder, time.milliseconds(), true);
                    return NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time);
                }
            } catch (Throwable e) {
                networkClient.close(sourceNode.idString());
                throw new RuntimeException(e);
            }
        }
    }
}
