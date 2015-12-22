/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is a  kafka 0.7 to 0.8 online migration tool used for migrating data from 0.7 to 0.8 cluster. Internally,
 * it's composed of a kafka 0.7 consumer and kafka 0.8 producer. The kafka 0.7 consumer consumes data from the
 * 0.7 cluster, and the kafka 0.8 producer produces data to the 0.8 cluster.
 *
 * The 0.7 consumer is loaded from kafka 0.7 jar using a "parent last, child first" java class loader.
 * Ordinary class loader is "parent first, child last", and kafka 0.8 and 0.7 both have classes for a lot of
 * class names like "kafka.consumer.Consumer", etc., so ordinary java URLClassLoader with kafka 0.7 jar will
 * will still load the 0.8 version class.
 *
 * As kafka 0.7 and kafka 0.8 used different version of zkClient, the zkClient jar used by kafka 0.7 should
 * also be used by the class loader.
 *
 * The user need to provide the configuration file for 0.7 consumer and 0.8 producer. For 0.8 producer,
 * the "serializer.class" config is set to "kafka.serializer.DefaultEncoder" by the code.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class KafkaMigrationTool {
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(KafkaMigrationTool.class.getName());
    private static final String KAFKA_07_STATIC_CONSUMER_CLASS_NAME = "kafka.consumer.Consumer";
    private static final String KAFKA_07_CONSUMER_CONFIG_CLASS_NAME = "kafka.consumer.ConsumerConfig";
    private static final String KAFKA_07_CONSUMER_STREAM_CLASS_NAME = "kafka.consumer.KafkaStream";
    private static final String KAFKA_07_CONSUMER_ITERATOR_CLASS_NAME = "kafka.consumer.ConsumerIterator";
    private static final String KAFKA_07_CONSUMER_CONNECTOR_CLASS_NAME = "kafka.javaapi.consumer.ConsumerConnector";
    private static final String KAFKA_07_MESSAGE_AND_METADATA_CLASS_NAME = "kafka.message.MessageAndMetadata";
    private static final String KAFKA_07_MESSAGE_CLASS_NAME = "kafka.message.Message";
    private static final String KAFKA_07_WHITE_LIST_CLASS_NAME = "kafka.consumer.Whitelist";
    private static final String KAFKA_07_TOPIC_FILTER_CLASS_NAME = "kafka.consumer.TopicFilter";
    private static final String KAFKA_07_BLACK_LIST_CLASS_NAME = "kafka.consumer.Blacklist";

    private static Class<?> kafkaStaticConsumer07 = null;
    private static Class<?> consumerConfig07 = null;
    private static Class<?> consumerConnector07 = null;
    private static Class<?> kafkaStream07 = null;
    private static Class<?> topicFilter07 = null;
    private static Class<?> whiteList07 = null;
    private static Class<?> blackList07 = null;
    private static Class<?> kafkaConsumerIteratorClass07 = null;
    private static Class<?> kafkaMessageAndMetaDataClass07 = null;
    private static Class<?> kafkaMessageClass07 = null;

    public static void main(String[] args) throws InterruptedException, IOException {
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> consumerConfigOpt
            = parser.accepts("consumer.config", "Kafka 0.7 consumer config to consume from the source 0.7 cluster. " + "You man specify multiple of these.")
            .withRequiredArg()
            .describedAs("config file")
            .ofType(String.class);

        ArgumentAcceptingOptionSpec<String> producerConfigOpt
            = parser.accepts("producer.config", "Producer config.")
            .withRequiredArg()
            .describedAs("config file")
            .ofType(String.class);

        ArgumentAcceptingOptionSpec<Integer> numProducersOpt
            = parser.accepts("num.producers", "Number of producer instances")
            .withRequiredArg()
            .describedAs("Number of producers")
            .ofType(Integer.class)
            .defaultsTo(1);

        ArgumentAcceptingOptionSpec<String> zkClient01JarOpt
            = parser.accepts("zkclient.01.jar", "zkClient 0.1 jar file")
            .withRequiredArg()
            .describedAs("zkClient 0.1 jar file required by Kafka 0.7")
            .ofType(String.class);

        ArgumentAcceptingOptionSpec<String> kafka07JarOpt
            = parser.accepts("kafka.07.jar", "Kafka 0.7 jar file")
            .withRequiredArg()
            .describedAs("kafka 0.7 jar")
            .ofType(String.class);

        ArgumentAcceptingOptionSpec<Integer> numStreamsOpt
            = parser.accepts("num.streams", "Number of consumer streams")
            .withRequiredArg()
            .describedAs("Number of consumer threads")
            .ofType(Integer.class)
            .defaultsTo(1);

        ArgumentAcceptingOptionSpec<String> whitelistOpt
            = parser.accepts("whitelist", "Whitelist of topics to migrate from the 0.7 cluster")
            .withRequiredArg()
            .describedAs("Java regex (String)")
            .ofType(String.class);

        ArgumentAcceptingOptionSpec<String> blacklistOpt
            = parser.accepts("blacklist", "Blacklist of topics to migrate from the 0.7 cluster")
            .withRequiredArg()
            .describedAs("Java regex (String)")
            .ofType(String.class);

        ArgumentAcceptingOptionSpec<Integer> queueSizeOpt
            = parser.accepts("queue.size", "Number of messages that are buffered between the 0.7 consumer and 0.8 producer")
            .withRequiredArg()
            .describedAs("Queue size in terms of number of messages")
            .ofType(Integer.class)
            .defaultsTo(10000);

        OptionSpecBuilder helpOpt
            = parser.accepts("help", "Print this message.");

        OptionSet options = parser.parse(args);

        if (options.has(helpOpt)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        checkRequiredArgs(parser, options, new OptionSpec[]{consumerConfigOpt, producerConfigOpt, zkClient01JarOpt, kafka07JarOpt});
        int whiteListCount = options.has(whitelistOpt) ? 1 : 0;
        int blackListCount = options.has(blacklistOpt) ? 1 : 0;
        if (whiteListCount + blackListCount != 1) {
            System.err.println("Exactly one of whitelist or blacklist is required.");
            System.exit(1);
        }

        String kafkaJarFile07 = options.valueOf(kafka07JarOpt);
        String zkClientJarFile = options.valueOf(zkClient01JarOpt);
        String consumerConfigFile07 = options.valueOf(consumerConfigOpt);
        int numConsumers = options.valueOf(numStreamsOpt);
        String producerConfigFile08 = options.valueOf(producerConfigOpt);
        int numProducers = options.valueOf(numProducersOpt);
        final List<MigrationThread> migrationThreads = new ArrayList<MigrationThread>(numConsumers);
        final List<ProducerThread> producerThreads = new ArrayList<ProducerThread>(numProducers);

        try {
            File kafkaJar07 = new File(kafkaJarFile07);
            File zkClientJar = new File(zkClientJarFile);
            ParentLastURLClassLoader c1 = new ParentLastURLClassLoader(new URL[]{
                kafkaJar07.toURI().toURL(),
                zkClientJar.toURI().toURL()
            });

            /** Construct the 07 consumer config **/
            consumerConfig07 = c1.loadClass(KAFKA_07_CONSUMER_CONFIG_CLASS_NAME);
            kafkaStaticConsumer07 = c1.loadClass(KAFKA_07_STATIC_CONSUMER_CLASS_NAME);
            consumerConnector07 = c1.loadClass(KAFKA_07_CONSUMER_CONNECTOR_CLASS_NAME);
            kafkaStream07 = c1.loadClass(KAFKA_07_CONSUMER_STREAM_CLASS_NAME);
            topicFilter07 = c1.loadClass(KAFKA_07_TOPIC_FILTER_CLASS_NAME);
            whiteList07 = c1.loadClass(KAFKA_07_WHITE_LIST_CLASS_NAME);
            blackList07 = c1.loadClass(KAFKA_07_BLACK_LIST_CLASS_NAME);
            kafkaMessageClass07 = c1.loadClass(KAFKA_07_MESSAGE_CLASS_NAME);
            kafkaConsumerIteratorClass07 = c1.loadClass(KAFKA_07_CONSUMER_ITERATOR_CLASS_NAME);
            kafkaMessageAndMetaDataClass07 = c1.loadClass(KAFKA_07_MESSAGE_AND_METADATA_CLASS_NAME);

            Constructor consumerConfigConstructor07 = consumerConfig07.getConstructor(Properties.class);
            Properties kafkaConsumerProperties07 = new Properties();
            kafkaConsumerProperties07.load(new FileInputStream(consumerConfigFile07));
            /** Disable shallow iteration because the message format is different between 07 and 08, we have to get each individual message **/
            if (kafkaConsumerProperties07.getProperty("shallow.iterator.enable", "").equals("true")) {
                log.warn("Shallow iterator should not be used in the migration tool");
                kafkaConsumerProperties07.setProperty("shallow.iterator.enable", "false");
            }
            Object consumerConfig07 = consumerConfigConstructor07.newInstance(kafkaConsumerProperties07);

            /** Construct the 07 consumer connector **/
            Method consumerConnectorCreationMethod07 = kafkaStaticConsumer07.getMethod("createJavaConsumerConnector", KafkaMigrationTool.consumerConfig07);
            final Object consumerConnector07 = consumerConnectorCreationMethod07.invoke(null, consumerConfig07);
            Method consumerConnectorCreateMessageStreamsMethod07 = KafkaMigrationTool.consumerConnector07.getMethod(
                "createMessageStreamsByFilter",
                topicFilter07, int.class);
            final Method consumerConnectorShutdownMethod07 = KafkaMigrationTool.consumerConnector07.getMethod("shutdown");
            Constructor whiteListConstructor07 = whiteList07.getConstructor(String.class);
            Constructor blackListConstructor07 = blackList07.getConstructor(String.class);
            Object filterSpec = null;
            if (options.has(whitelistOpt))
                filterSpec = whiteListConstructor07.newInstance(options.valueOf(whitelistOpt));
            else
                filterSpec = blackListConstructor07.newInstance(options.valueOf(blacklistOpt));

            Object retKafkaStreams = consumerConnectorCreateMessageStreamsMethod07.invoke(consumerConnector07, filterSpec, numConsumers);

            Properties kafkaProducerProperties08 = new Properties();
            kafkaProducerProperties08.load(new FileInputStream(producerConfigFile08));
            kafkaProducerProperties08.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
            // create a producer channel instead
            int queueSize = options.valueOf(queueSizeOpt);
            ProducerDataChannel<KeyedMessage<byte[], byte[]>> producerDataChannel = new ProducerDataChannel<KeyedMessage<byte[], byte[]>>(queueSize);
            int threadId = 0;

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        consumerConnectorShutdownMethod07.invoke(consumerConnector07);
                    } catch (Exception e) {
                        log.error("Error while shutting down Kafka consumer", e);
                    }
                    for (MigrationThread migrationThread : migrationThreads) {
                        migrationThread.shutdown();
                    }
                    for (ProducerThread producerThread : producerThreads) {
                        producerThread.shutdown();
                    }
                    for (ProducerThread producerThread : producerThreads) {
                        producerThread.awaitShutdown();
                    }
                    log.info("Kafka migration tool shutdown successfully");
                }
            });

            // start consumer threads
            for (Object stream : (List) retKafkaStreams) {
                MigrationThread thread = new MigrationThread(stream, producerDataChannel, threadId);
                threadId++;
                thread.start();
                migrationThreads.add(thread);
            }

            String clientId = kafkaProducerProperties08.getProperty("client.id");
            // start producer threads
            for (int i = 0; i < numProducers; i++) {
                kafkaProducerProperties08.put("client.id", clientId + "-" + i);
                ProducerConfig producerConfig08 = new ProducerConfig(kafkaProducerProperties08);
                Producer producer = new Producer(producerConfig08);
                ProducerThread producerThread = new ProducerThread(producerDataChannel, producer, i);
                producerThread.start();
                producerThreads.add(producerThread);
            }
        } catch (Throwable e) {
            System.out.println("Kafka migration tool failed due to: " + Utils.stackTrace(e));
            log.error("Kafka migration tool failed: ", e);
        }
    }

    private static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec[] required) throws IOException {
        for (OptionSpec arg : required) {
            if (!options.has(arg)) {
                System.err.println("Missing required argument \"" + arg + "\"");
                parser.printHelpOn(System.err);
                System.exit(1);
            }
        }
    }

    static class ProducerDataChannel<T> {
        private final int producerQueueSize;
        private final BlockingQueue<T> producerRequestQueue;

        public ProducerDataChannel(int queueSize) {
            producerQueueSize = queueSize;
            producerRequestQueue = new ArrayBlockingQueue<T>(producerQueueSize);
        }

        public void sendRequest(T data) throws InterruptedException {
            producerRequestQueue.put(data);
        }

        public T receiveRequest() throws InterruptedException {
            return producerRequestQueue.take();
        }
    }

    private static class MigrationThread extends Thread {
        private final Object stream;
        private final ProducerDataChannel<KeyedMessage<byte[], byte[]>> producerDataChannel;
        private final int threadId;
        private final String threadName;
        private final org.apache.log4j.Logger logger;
        private CountDownLatch shutdownComplete = new CountDownLatch(1);
        private final AtomicBoolean isRunning = new AtomicBoolean(true);

        MigrationThread(Object stream, ProducerDataChannel<KeyedMessage<byte[], byte[]>> producerDataChannel, int threadId) {
            this.stream = stream;
            this.producerDataChannel = producerDataChannel;
            this.threadId = threadId;
            threadName = "MigrationThread-" + threadId;
            logger = org.apache.log4j.Logger.getLogger(MigrationThread.class.getName());
            this.setName(threadName);
        }

        public void run() {
            try {
                Method messageGetPayloadMethod07 = kafkaMessageClass07.getMethod("payload");
                Method kafkaGetMessageMethod07 = kafkaMessageAndMetaDataClass07.getMethod("message");
                Method kafkaGetTopicMethod07 = kafkaMessageAndMetaDataClass07.getMethod("topic");
                Method consumerIteratorMethod = kafkaStream07.getMethod("iterator");
                Method kafkaStreamHasNextMethod07 = kafkaConsumerIteratorClass07.getMethod("hasNext");
                Method kafkaStreamNextMethod07 = kafkaConsumerIteratorClass07.getMethod("next");
                Object iterator = consumerIteratorMethod.invoke(stream);

                while (((Boolean) kafkaStreamHasNextMethod07.invoke(iterator)).booleanValue()) {
                    Object messageAndMetaData07 = kafkaStreamNextMethod07.invoke(iterator);
                    Object message07 = kafkaGetMessageMethod07.invoke(messageAndMetaData07);
                    Object topic = kafkaGetTopicMethod07.invoke(messageAndMetaData07);
                    Object payload07 = messageGetPayloadMethod07.invoke(message07);
                    int size = ((ByteBuffer) payload07).remaining();
                    byte[] bytes = new byte[size];
                    ((ByteBuffer) payload07).get(bytes);
                    if (logger.isDebugEnabled())
                        logger.debug("Migration thread " + threadId + " sending message of size " + bytes.length + " to topic " + topic);
                    KeyedMessage<byte[], byte[]> producerData = new KeyedMessage((String) topic, null, bytes);
                    producerDataChannel.sendRequest(producerData);
                }
                logger.info("Migration thread " + threadName + " finished running");
            } catch (InvocationTargetException t) {
                logger.fatal("Migration thread failure due to root cause ", t.getCause());
            } catch (Throwable t) {
                logger.fatal("Migration thread failure due to ", t);
            } finally {
                shutdownComplete.countDown();
            }
        }

        public void shutdown() {
            logger.info("Migration thread " + threadName + " shutting down");
            isRunning.set(false);
            interrupt();
            try {
                shutdownComplete.await();
            } catch (InterruptedException ie) {
                logger.warn("Interrupt during shutdown of MigrationThread", ie);
            }
            logger.info("Migration thread " + threadName + " shutdown complete");
        }
    }

    static class ProducerThread extends Thread {
        private final ProducerDataChannel<KeyedMessage<byte[], byte[]>> producerDataChannel;
        private final Producer<byte[], byte[]> producer;
        private final int threadId;
        private String threadName;
        private org.apache.log4j.Logger logger;
        private CountDownLatch shutdownComplete = new CountDownLatch(1);
        private KeyedMessage<byte[], byte[]> shutdownMessage = new KeyedMessage("shutdown", null, null);

        public ProducerThread(ProducerDataChannel<KeyedMessage<byte[], byte[]>> producerDataChannel,
                              Producer<byte[], byte[]> producer,
                              int threadId) {
            this.producerDataChannel = producerDataChannel;
            this.producer = producer;
            this.threadId = threadId;
            threadName = "ProducerThread-" + threadId;
            logger = org.apache.log4j.Logger.getLogger(ProducerThread.class.getName());
            this.setName(threadName);
        }

        public void run() {
            try {
                while (true) {
                    KeyedMessage<byte[], byte[]> data = producerDataChannel.receiveRequest();
                    if (!data.equals(shutdownMessage)) {
                        producer.send(data);
                        if (logger.isDebugEnabled())
                            logger.debug(String.format("Sending message %s", new String(data.message())));
                    } else
                        break;
                }
                logger.info("Producer thread " + threadName + " finished running");
            } catch (Throwable t) {
                logger.fatal("Producer thread failure due to ", t);
            } finally {
                shutdownComplete.countDown();
            }
        }

        public void shutdown() {
            try {
                logger.info("Producer thread " + threadName + " shutting down");
                producerDataChannel.sendRequest(shutdownMessage);
            } catch (InterruptedException ie) {
                logger.warn("Interrupt during shutdown of ProducerThread", ie);
            }
        }

        public void awaitShutdown() {
            try {
                shutdownComplete.await();
                producer.close();
                logger.info("Producer thread " + threadName + " shutdown complete");
            } catch (InterruptedException ie) {
                logger.warn("Interrupt during shutdown of ProducerThread", ie);
            }
        }
    }

    /**
     * A parent-last class loader that will try the child class loader first and then the parent.
     * This takes a fair bit of doing because java really prefers parent-first.
     */
    private static class ParentLastURLClassLoader extends ClassLoader {
        private ChildURLClassLoader childClassLoader;

        /**
         * This class allows me to call findClass on a class loader
         */
        private static class FindClassClassLoader extends ClassLoader {
            public FindClassClassLoader(ClassLoader parent) {
                super(parent);
            }

            @Override
            public Class<?> findClass(String name) throws ClassNotFoundException {
                return super.findClass(name);
            }
        }

        /**
         * This class delegates (child then parent) for the findClass method for a URLClassLoader.
         * We need this because findClass is protected in URLClassLoader
         */
        private static class ChildURLClassLoader extends URLClassLoader {
            private FindClassClassLoader realParent;

            public ChildURLClassLoader(URL[] urls, FindClassClassLoader realParent) {
                super(urls, null);
                this.realParent = realParent;
            }

            @Override
            public Class<?> findClass(String name) throws ClassNotFoundException {
                try {
                    // first try to use the URLClassLoader findClass
                    return super.findClass(name);
                } catch (ClassNotFoundException e) {
                    // if that fails, we ask our real parent class loader to load the class (we give up)
                    return realParent.loadClass(name);
                }
            }
        }

        public ParentLastURLClassLoader(URL[] urls) {
            super(Thread.currentThread().getContextClassLoader());
            childClassLoader = new ChildURLClassLoader(urls, new FindClassClassLoader(this.getParent()));
        }

        @Override
        protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            try {
                // first we try to find a class inside the child class loader
                return childClassLoader.findClass(name);
            } catch (ClassNotFoundException e) {
                // didn't find it, try the parent
                return super.loadClass(name, resolve);
            }
        }
    }
}

