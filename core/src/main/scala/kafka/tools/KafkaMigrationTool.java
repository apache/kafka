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

import joptsimple.*;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.Utils;

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
public class KafkaMigrationTool
{
  private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(KafkaMigrationTool.class.getName());
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

  private static Class<?> KafkaStaticConsumer_07 = null;
  private static Class<?> ConsumerConfig_07 = null;
  private static Class<?> ConsumerConnector_07 = null;
  private static Class<?> KafkaStream_07 = null;
  private static Class<?> TopicFilter_07 = null;
  private static Class<?> WhiteList_07 = null;
  private static Class<?> BlackList_07 = null;
  private static Class<?> KafkaConsumerIteratorClass_07 = null;
  private static Class<?> KafkaMessageAndMetatDataClass_07 = null;
  private static Class<?> KafkaMessageClass_07 = null;

  public static void main(String[] args) throws InterruptedException, IOException {
    OptionParser parser = new OptionParser();
    ArgumentAcceptingOptionSpec<String> consumerConfigOpt
      = parser.accepts("consumer.config", "Kafka 0.7 consumer config to consume from the source 0.7 cluster. " + "You man specify multiple of these.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> producerConfigOpt
      =  parser.accepts("producer.config", "Producer config.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(String.class);

    ArgumentAcceptingOptionSpec<Integer> numProducersOpt
      =  parser.accepts("num.producers", "Number of producer instances")
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
      =  parser.accepts("queue.size", "Number of messages that are buffered between the 0.7 consumer and 0.8 producer")
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
    if(whiteListCount + blackListCount != 1) {
      System.err.println("Exactly one of whitelist or blacklist is required.");
      System.exit(1);
    }

    String kafkaJarFile_07 = options.valueOf(kafka07JarOpt);
    String zkClientJarFile = options.valueOf(zkClient01JarOpt);
    String consumerConfigFile_07 = options.valueOf(consumerConfigOpt);
    int numConsumers = options.valueOf(numStreamsOpt);
    String producerConfigFile_08 = options.valueOf(producerConfigOpt);
    int numProducers = options.valueOf(numProducersOpt);
    final List<MigrationThread> migrationThreads = new ArrayList<MigrationThread>(numConsumers);
    final List<ProducerThread> producerThreads = new ArrayList<ProducerThread>(numProducers);

    try {
      File kafkaJar_07 = new File(kafkaJarFile_07);
      File zkClientJar = new File(zkClientJarFile);
      ParentLastURLClassLoader c1 = new ParentLastURLClassLoader(new URL[] {
        kafkaJar_07.toURI().toURL(),
        zkClientJar.toURI().toURL()
      });

      /** Construct the 07 consumer config **/
      ConsumerConfig_07 = c1.loadClass(KAFKA_07_CONSUMER_CONFIG_CLASS_NAME);
      KafkaStaticConsumer_07 = c1.loadClass(KAFKA_07_STATIC_CONSUMER_CLASS_NAME);
      ConsumerConnector_07 = c1.loadClass(KAFKA_07_CONSUMER_CONNECTOR_CLASS_NAME);
      KafkaStream_07 = c1.loadClass(KAFKA_07_CONSUMER_STREAM_CLASS_NAME);
      TopicFilter_07 = c1.loadClass(KAFKA_07_TOPIC_FILTER_CLASS_NAME);
      WhiteList_07 = c1.loadClass(KAFKA_07_WHITE_LIST_CLASS_NAME);
      BlackList_07 = c1.loadClass(KAFKA_07_BLACK_LIST_CLASS_NAME);
      KafkaMessageClass_07 = c1.loadClass(KAFKA_07_MESSAGE_CLASS_NAME);
      KafkaConsumerIteratorClass_07 = c1.loadClass(KAFKA_07_CONSUMER_ITERATOR_CLASS_NAME);
      KafkaMessageAndMetatDataClass_07 = c1.loadClass(KAFKA_07_MESSAGE_AND_METADATA_CLASS_NAME);

      Constructor ConsumerConfigConstructor_07 = ConsumerConfig_07.getConstructor(Properties.class);
      Properties kafkaConsumerProperties_07 = new Properties();
      kafkaConsumerProperties_07.load(new FileInputStream(consumerConfigFile_07));
      /** Disable shallow iteration because the message format is different between 07 and 08, we have to get each individual message **/
      if(kafkaConsumerProperties_07.getProperty("shallow.iterator.enable", "").equals("true")) {
        logger.warn("Shallow iterator should not be used in the migration tool");
        kafkaConsumerProperties_07.setProperty("shallow.iterator.enable", "false");
      }
      Object consumerConfig_07 = ConsumerConfigConstructor_07.newInstance(kafkaConsumerProperties_07);

      /** Construct the 07 consumer connector **/
      Method ConsumerConnectorCreationMethod_07 = KafkaStaticConsumer_07.getMethod("createJavaConsumerConnector", ConsumerConfig_07);
      final Object consumerConnector_07 = ConsumerConnectorCreationMethod_07.invoke(null, consumerConfig_07);
      Method ConsumerConnectorCreateMessageStreamsMethod_07 = ConsumerConnector_07.getMethod(
        "createMessageStreamsByFilter",
        TopicFilter_07, int.class);
      final Method ConsumerConnectorShutdownMethod_07 = ConsumerConnector_07.getMethod("shutdown");
      Constructor WhiteListConstructor_07 = WhiteList_07.getConstructor(String.class);
      Constructor BlackListConstructor_07 = BlackList_07.getConstructor(String.class);
      Object filterSpec = null;
      if(options.has(whitelistOpt))
        filterSpec = WhiteListConstructor_07.newInstance(options.valueOf(whitelistOpt));
      else
        filterSpec = BlackListConstructor_07.newInstance(options.valueOf(blacklistOpt));

      Object retKafkaStreams = ConsumerConnectorCreateMessageStreamsMethod_07.invoke(consumerConnector_07, filterSpec, numConsumers);

      Properties kafkaProducerProperties_08 = new Properties();
      kafkaProducerProperties_08.load(new FileInputStream(producerConfigFile_08));
      kafkaProducerProperties_08.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
      // create a producer channel instead
      int queueSize = options.valueOf(queueSizeOpt);
      ProducerDataChannel<KeyedMessage<String, byte[]>> producerDataChannel = new ProducerDataChannel<KeyedMessage<String, byte[]>>(queueSize);
      int threadId = 0;

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          try {
            ConsumerConnectorShutdownMethod_07.invoke(consumerConnector_07);
          } catch(Exception e) {
            logger.error("Error while shutting down Kafka consumer", e);
          }
          for(MigrationThread migrationThread : migrationThreads) {
            migrationThread.shutdown();
          }
          for(ProducerThread producerThread : producerThreads) {
            producerThread.shutdown();
          }
          for(ProducerThread producerThread : producerThreads) {
            producerThread.awaitShutdown();
          }
          logger.info("Kafka migration tool shutdown successfully");
        }
      });

      // start consumer threads
      for(Object stream: (List)retKafkaStreams) {
        MigrationThread thread = new MigrationThread(stream, producerDataChannel, threadId);
        threadId ++;
        thread.start();
        migrationThreads.add(thread);
      }

      String clientId = kafkaProducerProperties_08.getProperty("client.id");
      // start producer threads
      for (int i = 0; i < numProducers; i++) {
        kafkaProducerProperties_08.put("client.id", clientId + "-" + i);
        ProducerConfig producerConfig_08 = new ProducerConfig(kafkaProducerProperties_08);
        Producer producer = new Producer(producerConfig_08);
        ProducerThread producerThread = new ProducerThread(producerDataChannel, producer, i);
        producerThread.start();
        producerThreads.add(producerThread);
      }
    }
    catch (Throwable e){
      System.out.println("Kafka migration tool failed due to: " + Utils.stackTrace(e));
      logger.error("Kafka migration tool failed: ", e);
    }
  }

  private static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec[] required) throws IOException {
    for(OptionSpec arg : required) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"");
        parser.printHelpOn(System.err);
        System.exit(1);
      }
    }
  }


  private static class ProducerDataChannel<T> {
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
    private final ProducerDataChannel<KeyedMessage<String, byte[]>> producerDataChannel;
    private final int threadId;
    private final String threadName;
    private final org.apache.log4j.Logger logger;
    private CountDownLatch shutdownComplete = new CountDownLatch(1);
    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    MigrationThread(Object _stream, ProducerDataChannel<KeyedMessage<String, byte[]>> _producerDataChannel, int _threadId) {
      stream = _stream;
      producerDataChannel = _producerDataChannel;
      threadId = _threadId;
      threadName = "MigrationThread-" + threadId;
      logger = org.apache.log4j.Logger.getLogger(MigrationThread.class.getName());
      this.setName(threadName);
    }

    public void run() {
      try {
        Method MessageGetPayloadMethod_07 = KafkaMessageClass_07.getMethod("payload");
        Method KafkaGetMessageMethod_07 = KafkaMessageAndMetatDataClass_07.getMethod("message");
        Method KafkaGetTopicMethod_07 = KafkaMessageAndMetatDataClass_07.getMethod("topic");
        Method ConsumerIteratorMethod = KafkaStream_07.getMethod("iterator");
        Method KafkaStreamHasNextMethod_07 = KafkaConsumerIteratorClass_07.getMethod("hasNext");
        Method KafkaStreamNextMethod_07 = KafkaConsumerIteratorClass_07.getMethod("next");
        Object iterator = ConsumerIteratorMethod.invoke(stream);

        while (((Boolean) KafkaStreamHasNextMethod_07.invoke(iterator)).booleanValue()) {
          Object messageAndMetaData_07 = KafkaStreamNextMethod_07.invoke(iterator);
          Object message_07 = KafkaGetMessageMethod_07.invoke(messageAndMetaData_07);
          Object topic = KafkaGetTopicMethod_07.invoke(messageAndMetaData_07);
          Object payload_07 = MessageGetPayloadMethod_07.invoke(message_07);
          int size = ((ByteBuffer)payload_07).remaining();
          byte[] bytes = new byte[size];
          ((ByteBuffer)payload_07).get(bytes);
          if(logger.isDebugEnabled())
            logger.debug("Migration thread " + threadId + " sending message of size " + bytes.length + " to topic "+ topic);
          KeyedMessage<String, byte[]> producerData = new KeyedMessage((String)topic, null, bytes);
          producerDataChannel.sendRequest(producerData);
        }
        logger.info("Migration thread " + threadName + " finished running");
      } catch (InvocationTargetException t){
        logger.fatal("Migration thread failure due to root cause ", t.getCause());
      } catch (Throwable t){
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
      } catch(InterruptedException ie) {
        logger.warn("Interrupt during shutdown of MigrationThread", ie);
      }
      logger.info("Migration thread " + threadName + " shutdown complete");
    }
  }

  private static class ProducerThread extends Thread {
    private final ProducerDataChannel<KeyedMessage<String, byte[]>> producerDataChannel;
    private final Producer<String, byte[]> producer;
    private final int threadId;
    private String threadName;
    private org.apache.log4j.Logger logger;
    private CountDownLatch shutdownComplete = new CountDownLatch(1);
    private KeyedMessage<String, byte[]> shutdownMessage = new KeyedMessage("shutdown", null, null);

    public ProducerThread(ProducerDataChannel<KeyedMessage<String, byte[]>> _producerDataChannel,
                          Producer<String, byte[]> _producer,
                          int _threadId) {
      producerDataChannel = _producerDataChannel;
      producer = _producer;
      threadId = _threadId;
      threadName = "ProducerThread-" + threadId;
      logger = org.apache.log4j.Logger.getLogger(ProducerThread.class.getName());
      this.setName(threadName);
    }

    public void run() {
      try{
        while(true) {
          KeyedMessage<String, byte[]> data = producerDataChannel.receiveRequest();
          if(!data.equals(shutdownMessage))
            producer.send(data);
          else
            break;
        }
        logger.info("Producer thread " + threadName + " finished running");
      } catch (Throwable t){
        logger.fatal("Producer thread failure due to ", t);
      } finally {
        shutdownComplete.countDown();
      }
    }

    public void shutdown() {
      try {
        logger.info("Producer thread " + threadName + " shutting down");
        producerDataChannel.sendRequest(shutdownMessage);
      } catch(InterruptedException ie) {
        logger.warn("Interrupt during shutdown of ProducerThread", ie);
      }
    }

    public void awaitShutdown() {
      try {
        shutdownComplete.await();
        logger.info("Producer thread " + threadName + " shutdown complete");
      } catch(InterruptedException ie) {
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
      public ChildURLClassLoader( URL[] urls, FindClassClassLoader realParent) {
        super(urls, null);
        this.realParent = realParent;
      }

      @Override
      public Class<?> findClass(String name) throws ClassNotFoundException {
        try{
          // first try to use the URLClassLoader findClass
          return super.findClass(name);
        }
        catch( ClassNotFoundException e ) {
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
      }
      catch( ClassNotFoundException e ) {
        // didn't find it, try the parent
        return super.loadClass(name, resolve);
      }
    }
  }
}

