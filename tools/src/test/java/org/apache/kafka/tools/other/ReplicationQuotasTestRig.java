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
package org.apache.kafka.tools.other;

import kafka.log.UnifiedLog;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.QuorumTestHarness;
import kafka.server.QuotaType;
import kafka.utils.EmptyTestInfo;
import kafka.utils.Exit;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.tools.reassign.ReassignPartitionsCommand;
import org.apache.log4j.PropertyConfigurator;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import javax.imageio.ImageIO;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;

/**
 * Test rig for measuring throttling performance. Configure the parameters for a set of experiments, then execute them
 * and view the html output file, with charts, that are produced. You can also render the charts to the screen if
 * you wish.
 *
 * Currently you'll need about 40GB of disk space to run these experiments (largest data written x2). Tune the msgSize
 * & #partitions and throttle to adjust this.
 */
public class ReplicationQuotasTestRig {
    public static final Logger LOGGER = LoggerFactory.getLogger(ReplicationQuotasTestRig.class);

    public static final int K = 1000 * 1000;

    private static final String DIR;

    static {
        PropertyConfigurator.configure("core/src/test/resources/log4j.properties");

        new File("Experiments").mkdir();
        DIR = "Experiments/Run" + Long.valueOf(System.currentTimeMillis()).toString().substring(8);
        new File(DIR).mkdir();
    }

    public static void main(String[] args) {
        boolean displayChartsOnScreen = args.length > 0 && Objects.equals(args[0], "show-gui");
        Journal journal = new Journal();

        List<ExperimentDef> experiments = Arrays.asList(
            //1GB total data written, will take 210s
            new ExperimentDef("Experiment1", 5, 20, 1 * K, 500, 100 * 1000),
            //5GB total data written, will take 110s
            new ExperimentDef("Experiment2", 5, 50, 10 * K, 1000, 100 * 1000),
            //5GB total data written, will take 110s
            new ExperimentDef("Experiment3", 50, 50, 2 * K, 1000, 100 * 1000),
            //10GB total data written, will take 110s
            new ExperimentDef("Experiment4", 25, 100, 4 * K, 1000, 100 * 1000),
            //10GB total data written, will take 80s
            new ExperimentDef("Experiment5", 5, 50, 50 * K, 4000, 100 * 1000)
        );
        experiments.forEach(def -> run(def, journal, displayChartsOnScreen));

        if (!displayChartsOnScreen)
            Exit.exit(0, Option.empty());
    }

    static void run(ExperimentDef config, Journal journal, boolean displayChartsOnScreen) {
        Experiment experiment = new Experiment();
        try {
            experiment.setUp(new EmptyTestInfo());
            experiment.run(config, journal, displayChartsOnScreen);
            journal.footer();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            experiment.tearDown();
        }
    }

    static class ExperimentDef {
        String name;
        int brokers;
        int partitions;
        long throttle;
        int msgsPerPartition;
        int msgSize;
        final long targetBytesPerBrokerMB;

        public ExperimentDef(String name, int brokers, int partitions, long throttle, int msgsPerPartition, int msgSize) {
            this.name = name;
            this.brokers = brokers;
            this.partitions = partitions;
            this.throttle = throttle;
            this.msgsPerPartition = msgsPerPartition;
            this.msgSize = msgSize;
            this.targetBytesPerBrokerMB = (long) msgsPerPartition * (long) msgSize * (long) partitions / brokers / 1_000_000;
        }
    }

    static class Experiment extends QuorumTestHarness {
        static final String TOPIC_NAME = "my-topic";

        String experimentName = "unset";
        List<KafkaServer> servers;
        Map<Integer, List<Double>> leaderRates = new HashMap<>();
        Map<Integer, List<Double>> followerRates = new HashMap<>();
        Admin adminClient;

        void startBrokers(List<Integer> brokerIds) {
            System.out.println("Starting Brokers");
            servers = brokerIds.stream().map(i -> createBrokerConfig(i, zkConnect()))
                .map(c -> TestUtils.createServer(KafkaConfig.fromProps(c), Time.SYSTEM))
                .collect(Collectors.toList());

            TestUtils.waitUntilBrokerMetadataIsPropagated(seq(servers), DEFAULT_MAX_WAIT_MS);
            String brokerList = TestUtils.plaintextBootstrapServers(seq(servers));
            adminClient = Admin.create(Collections.singletonMap(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList
            ));
        }

        @Override public void tearDown() {
            Utils.closeQuietly(adminClient, "adminClient");
            TestUtils.shutdownServers(seq(servers), true);
            super.tearDown();
        }

        @SuppressWarnings({"unchecked", "deprecation"})
        public void run(ExperimentDef config, Journal journal, boolean displayChartsOnScreen) throws Exception {
            experimentName = config.name;
            List<Integer> brokers = IntStream.rangeClosed(100, 100 + config.brokers).boxed().collect(Collectors.toList());
            int shift = Math.round(config.brokers / 2f);

            IntSupplier nextReplicaRoundRobin = new IntSupplier() {
                int count = 0;

                @Override public int getAsInt() {
                    count++;
                    return 100 + (count + shift) % config.brokers;
                }
            };

            Map<Integer, Seq<Integer>> replicas = IntStream.rangeClosed(0, config.partitions).boxed().collect(Collectors.toMap(
                Function.identity(),
                partition -> seq(Collections.singleton(nextReplicaRoundRobin.getAsInt()))
            ));

            startBrokers(brokers);
            TestUtils.createTopic(zkClient(), TOPIC_NAME, (scala.collection.Map) JavaConverters.mapAsScalaMap(replicas), seq(servers));

            System.out.println("Writing Data");
            KafkaProducer<byte[], byte[]> producer = createProducer(TestUtils.plaintextBootstrapServers(seq(servers)));

            for (int x = 0; x < config.msgsPerPartition; x++) {
                for (int partition = 0; partition < config.partitions; partition++) {
                    producer.send(new ProducerRecord<>(TOPIC_NAME, partition, null, new byte[config.msgSize]));
                }
            }

            System.out.println("Generating Reassignment");
            Map<TopicPartition, List<Integer>> newAssignment = ReassignPartitionsCommand.generateAssignment(adminClient,
                json(TOPIC_NAME), brokers.stream().map(Object::toString).collect(Collectors.joining(",")), true).getKey();

            System.out.println("Starting Reassignment");
            long start = System.currentTimeMillis();

            ReassignPartitionsCommand.executeAssignment(adminClient, false,
                ReassignPartitionsCommand.formatAsReassignmentJson(newAssignment, Collections.emptyMap()),
                config.throttle, -1L, 10000L, Time.SYSTEM);

            //Await completion
            waitForReassignmentToComplete();
            System.out.println("Reassignment took " + (System.currentTimeMillis() - start) / 1000 + "s");

            validateAllOffsetsMatch(config);

            journal.appendToJournal(config);
            renderChart(leaderRates, "Leader", journal, displayChartsOnScreen);
            renderChart(followerRates, "Follower", journal, displayChartsOnScreen);
            logOutput(config, replicas, newAssignment);

            System.out.println("Output can be found here: " + journal.path());
        }

        void validateAllOffsetsMatch(ExperimentDef config) {
            //Validate that offsets are correct in all brokers
            for (KafkaServer broker : servers) {
                for (int partitionId = 0; partitionId < config.partitions; partitionId++) {
                    long offset = broker.getLogManager().getLog(new TopicPartition(TOPIC_NAME, partitionId), false).map(UnifiedLog::logEndOffset).getOrElse(() -> -1L);
                    if (offset >= 0 && offset != config.msgsPerPartition) {
                        throw new RuntimeException(
                            "Run failed as offsets did not match for partition " + partitionId + " on broker " + broker.config().brokerId() + ". " +
                            "Expected " + config.msgsPerPartition + " but was " + offset + "."
                        );
                    }
                }
            }
        }

        void logOutput(ExperimentDef config, Map<Integer, Seq<Integer>> replicas, Map<TopicPartition, List<Integer>> newAssignment) throws Exception {
            List<TopicPartitionInfo> actual = adminClient.describeTopics(Collections.singleton(TOPIC_NAME))
                .allTopicNames().get().get(TOPIC_NAME).partitions();

            Map<Integer, List<Integer>> curAssignment = actual.stream().collect(Collectors.toMap(
                TopicPartitionInfo::partition,
                p -> p.replicas().stream().map(Node::id).collect(Collectors.toList())
            ));

            //Long stats
            System.out.println("The replicas are " + new TreeMap<>(replicas).entrySet().stream().map(e -> "\n" + e).collect(Collectors.joining()));
            System.out.println("This is the current replica assignment:\n" + curAssignment);
            System.out.println("proposed assignment is: \n" + newAssignment);
            System.out.println("This is the assignment we ended up with" + curAssignment);

            //Test Stats
            System.out.println("numBrokers: " + config.brokers);
            System.out.println("numPartitions: " + config.partitions);
            System.out.println("throttle: " + config.throttle);
            System.out.println("numMessagesPerPartition: " + config.msgsPerPartition);
            System.out.println("msgSize: " + config.msgSize);
            System.out.println("We will write " + config.targetBytesPerBrokerMB + "MB of data per broker");
            System.out.println("Worst case duration is " + config.targetBytesPerBrokerMB * 1000 * 1000 / config.throttle);
        }

        void waitForReassignmentToComplete() {
            TestUtils.waitUntilTrue(() -> {
                printRateMetrics();
                try {
                    return adminClient.listPartitionReassignments().reassignments().get().isEmpty();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }, () -> "Partition reassignments didn't complete.", 60 * 60 * 1000, 1000L);
        }

        void renderChart(Map<Integer, List<Double>> data, String name, Journal journal, boolean displayChartsOnScreen) throws Exception {
            XYSeriesCollection dataset = addDataToChart(data);
            JFreeChart chart = createChart(name, dataset);

            writeToFile(name, journal, chart);
            maybeDisplayOnScreen(displayChartsOnScreen, chart);
            System.out.println("Chart generated for " + name);
        }

        void maybeDisplayOnScreen(boolean displayChartsOnScreen, JFreeChart chart) {
            if (displayChartsOnScreen) {
                ChartFrame frame = new ChartFrame(experimentName, chart);
                frame.pack();
                frame.setVisible(true);
            }
        }

        void writeToFile(String name, Journal journal, JFreeChart chart) throws Exception {
            File file = new File(DIR, experimentName + "-" + name + ".png");
            ImageIO.write(chart.createBufferedImage(1000, 700), "png", file);
            journal.appendChart(file.getAbsolutePath(), name.equals("Leader"));
        }

        JFreeChart createChart(String name, XYSeriesCollection dataset) {
            return ChartFactory.createXYLineChart(
                experimentName + " - " + name + " Throttling Performance",
                "Time (s)",
                "Throttle Throughput (B/s)",
                dataset, PlotOrientation.VERTICAL, false, true, false
            );
        }

        XYSeriesCollection addDataToChart(Map<Integer, List<Double>> data) {
            XYSeriesCollection dataset = new XYSeriesCollection();
            data.forEach((broker, values) -> {
                XYSeries series = new XYSeries("Broker:" + broker);
                int x = 0;
                for (double value : values) {
                    series.add(x, value);
                    x++;
                }
                dataset.addSeries(series);
            });
            return dataset;
        }

        void record(Map<Integer, List<Double>> rates, int brokerId, Double currentRate) {
            List<Double> leaderRatesBroker = rates.getOrDefault(brokerId, new ArrayList<>());
            leaderRatesBroker.add(currentRate);
            rates.put(brokerId, leaderRatesBroker);
        }

        void printRateMetrics() {
            for (KafkaServer broker : servers) {
                double leaderRate = measuredRate(broker, QuotaType.LeaderReplication$.MODULE$);
                if (broker.config().brokerId() == 100)
                    LOGGER.info("waiting... Leader rate on 101 is " + leaderRate);
                record(leaderRates, broker.config().brokerId(), leaderRate);
                if (leaderRate > 0)
                    LOGGER.trace("Leader Rate on " + broker.config().brokerId() + " is " + leaderRate);

                double followerRate = measuredRate(broker, QuotaType.FollowerReplication$.MODULE$);
                record(followerRates, broker.config().brokerId(), followerRate);
                if (followerRate > 0)
                    LOGGER.trace("Follower Rate on " + broker.config().brokerId() + " is " + followerRate);
            }
        }

        private double measuredRate(KafkaServer broker, QuotaType repType) {
            MetricName metricName = broker.metrics().metricName("byte-rate", repType.toString());
            return broker.metrics().metrics().containsKey(metricName)
                ? (double) broker.metrics().metrics().get(metricName).metricValue()
                : -1d;
        }

        String json(String... topic) {
            String topicStr = Arrays.stream(topic).map(t -> "{\"topic\": \"" + t + "\"}").collect(Collectors.joining(","));
            return "{\"topics\": [" + topicStr + "],\"version\":1}";
        }

        KafkaProducer<byte[], byte[]> createProducer(String brokerList) {
            return TestUtils.createProducer(
                brokerList,
                0,
                60 * 1000L,
                1024L * 1024L,
                Integer.MAX_VALUE,
                30 * 1000,
                0,
                16384,
                "none",
                20 * 1000,
                SecurityProtocol.PLAINTEXT,
                Option.empty(),
                Option.empty(),
                new ByteArraySerializer(),
                new ByteArraySerializer(),
                false
            );
        }

        Properties createBrokerConfig(int brokerId, String zkConnect) {
            return TestUtils.createBrokerConfig(
                brokerId,
                zkConnect,
                false, // shorten test time
                true,
                TestUtils.RandomPort(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                true,
                false,
                TestUtils.RandomPort(),
                false,
                TestUtils.RandomPort(),
                false,
                TestUtils.RandomPort(),
                Option.empty(),
                3,
                false,
                1,
                (short) 1,
                false);
        }
    }

    static class Journal {
        File log = new File(DIR, "Log.html");

        public Journal() {
            header();
        }

        void appendToJournal(ExperimentDef config) {
            DecimalFormat format = new DecimalFormat("###,###.###");

            String message = "\n\n<h3>" + config.name + "</h3>" +
                "<p>- BrokerCount: " + config.brokers +
                "<p>- PartitionCount: " + config.partitions +
                "<p>- Throttle: " + format.format(config.throttle) + " MB/s" +
                "<p>- MsgCount: " + format.format(config.msgsPerPartition) + " " +
                "<p>- MsgSize: " + format.format(config.msgSize) +
                "<p>- TargetBytesPerBrokerMB: " + config.targetBytesPerBrokerMB + "<p>";

            append(message);
        }

        void appendChart(String path, boolean first) {
            StringBuilder message = new StringBuilder();
            if (first)
                message.append("<p><p>");
            message.append("<img src=\"" + path + "\" alt=\"Chart\" style=\"width:600px;height:400px;align=\"middle\"\">");
            if (!first)
                message.append("<p><p>");
            append(message.toString());
        }

        void header() {
            append("<html><head><h1>Replication Quotas Test Rig</h1></head><body>");
        }

        void footer() {
            append("</body></html>");
        }

        void append(String message) {
            try {
                OutputStream stream = Files.newOutputStream(log.toPath(), CREATE, APPEND);
                PrintWriter writer = new PrintWriter(stream);
                writer.append(message);
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        String path() {
            return log.getAbsolutePath();
        }
    }

    @SuppressWarnings({"deprecation"})
    private static <T> Seq<T> seq(Collection<T> seq) {
        return JavaConverters.asScalaIteratorConverter(seq.iterator()).asScala().toSeq();
    }
}
