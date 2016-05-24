/**
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.api

import java.io.Closeable
import java.io.File
import java.net.URL
import java.net.URLClassLoader
import java.util.Collections
import java.util.Map
import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.RoundRobinAssignor
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.auth.DefaultPrincipalBuilder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.test.MockConsumerInterceptor
import org.apache.kafka.test.MockProducerInterceptor
import org.junit.{Before, After, Test}
import org.junit.Assert._

import kafka.server.KafkaConfig
import kafka.utils.TestUtils

class ClassLoaderTest extends IntegrationTestHarness with SaslSetup {

  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override val producerCount = 0
  override val consumerCount = 0
  override val serverCount = 1
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "false")

  val topic = "topic"
  val messageCount = 10

  val classLoaders = Buffer[TestClassLoader]()
  val producerClassLoader = new TestClassLoader
  val consumerClassLoader = new TestClassLoader
  val clients = Buffer[Closeable]()
  val tccl = Thread.currentThread.getContextClassLoader

  @Before
  override def setUp() {
    startSasl(KafkaSasl, List("GSSAPI"), List("GSSAPI"))
    super.setUp()
    TestUtils.createTopic(this.zkUtils, topic, 2, serverCount, this.servers)
    resetThreadContextClassLoader()
  }

  @After
  override def tearDown() {
    clients.foreach(_.close())
    super.tearDown()
    closeSasl()
    classLoaders.foreach(_.close())
    Thread.currentThread.setContextClassLoader(tccl)
  }

  /**
   * Tests producers and consumers running with default properties running with
   * a custom classloader. This is similar to running producer/consumer from
   * an OSGi bundle of Kafka.
   */
  @Test
  def testDefaultProperties() {
    runProducer(false, false, producerClassLoader, producerClassLoader)
    runConsumer(false, false, consumerClassLoader, consumerClassLoader)
  }

  /**
   * Tests producers and consumers running with custom classes for configurable
   * classes where the custom classes are loaded using the same classloader as Kafka.
   * This is similar to running producer/consumer with additional custom classes
   * contained in the same OSGi bundle as Kafka.
   */
  @Test
  def testPropertyOverrides() {
    runProducer(true, false, producerClassLoader, producerClassLoader)
    runConsumer(true, false, consumerClassLoader, consumerClassLoader)
  }

  /**
   * Tests producers and consumers running with custom classes for configurable
   * classes where the custom classes are loaded using a different classloader.
   * This is similar to running producer/consumer with additional custom classes
   * contained in a separate OSGi bundle. Kafka classloader does not have visibility
   * of the custom classes.
   */
  @Test
  def testCustomClassLoaderForConfigurableClasses() {
    runProducer(true, false, producerClassLoader, new TestClassLoader(delegate = producerClassLoader))
    runConsumer(true, false, consumerClassLoader, new TestClassLoader(delegate = consumerClassLoader))
  }

  /**
   * Tests producers and consumers with custom classes specified as class names.
   * Custom classes are loaded by Kafka using the thread context classloader.
   */
  @Test
  def testThreadContextClassLoader() {
    val kafkaClassLoader = new TestClassLoader
    val contextClassLoader = new TestClassLoader(delegate = kafkaClassLoader)
    Thread.currentThread.setContextClassLoader(contextClassLoader)
    runProducer(true, true, kafkaClassLoader, contextClassLoader)
    runConsumer(true, true, kafkaClassLoader, contextClassLoader)
  }

  /**
   * Tests producers and consumers with custom classes specified as class names
   * when thread context classloader is not set. Custom classes are loaded by Kafka
   * using the classloader that loaded Kafka.
   */
  @Test
  def testThreadContextClassLoaderNotSet() {
    val kafkaClassLoader = new TestClassLoader
    Thread.currentThread.setContextClassLoader(null)
    runProducer(true, true, kafkaClassLoader, kafkaClassLoader)
    runConsumer(true, true, kafkaClassLoader, kafkaClassLoader)
  }

  private def resetThreadContextClassLoader() {
    Thread.currentThread.setContextClassLoader(new TestClassLoader(new Array[URL](0)))
    try {
      Thread.currentThread.getContextClassLoader.loadClass(classOf[StringSerializer].getName)
      fail("Context classloader not reset correctly")
    } catch {
      case e: ClassNotFoundException => // Expected exception
    }
  }

  private def runProducer(overrideDefaults: Boolean, useClassNames: Boolean, classLoader: ClassLoader, extClassLoader: ClassLoader) {
    val producerProps = classLoaderSafeProperties(producerProperties(overrideDefaults), extClassLoader, useClassNames)
    runTest(classLoader, classOf[TestProducer], producerProps, topic, messageCount)
  }

  private def runConsumer(overrideDefaults: Boolean, useClassNames: Boolean, classLoader: ClassLoader, extClassLoader: ClassLoader) {
    val consumerProps =  classLoaderSafeProperties(consumerProperties(overrideDefaults), extClassLoader, useClassNames)
    runTest(classLoader, classOf[TestConsumer], consumerProps, topic, messageCount)
  }

  private def runTest(classLoader: ClassLoader, testClass: Class[_], properties: Map[String, Object], topic: String, numRecords: Integer) {
    val newTestClass = classLoader.loadClass(testClass.getName)
    val constructor = newTestClass.getConstructor(classOf[Map[String, Object]], classOf[String], classOf[Int])
    val testClient = constructor.newInstance(properties, topic, numRecords)
    clients += testClient.asInstanceOf[Closeable]
    testClient.asInstanceOf[Runnable].run()
  }

  private def producerProperties(overrideDefaults: Boolean) : Properties = {
    val producerProps = new Properties
    producerProps.putAll(TestUtils.producerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties))
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.ACKS_CONFIG, "1")
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer")
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[TestSerializer])
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[TestSerializer])
    if (overrideDefaults) {
      producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[TestPartitioner])
      producerProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Collections.singletonList(classOf[TestProducerInterceptor]))
      producerProps.put(SslConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[TestPrincipalBuilder])
      producerProps.put(MockProducerInterceptor.APPEND_STRING_PROP, "")
    }
    producerProps
  }

  private def consumerProperties(overrideDefaults: Boolean) : Properties = {
    val consumerProps = new Properties
    consumerProps.putAll(TestUtils.consumerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties))
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[TestDeserializer])
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[TestDeserializer])
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer")
    if (overrideDefaults) {
      consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, Collections.singletonList(classOf[TestPartitionAssignor]))
      consumerProps.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, Collections.singletonList(classOf[TestConsumerInterceptor]))
      consumerProps.put(SslConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[TestPrincipalBuilder])
    }
    consumerProps
  }

  def classLoaderSafeProperties(props: Properties, classLoader: ClassLoader, useClassName: Boolean) : Map[String, Object] = {
    def classPropValue(clazz: Class[_]) = if (useClassName) clazz.getName else classLoader.loadClass(clazz.getName)
    def password(value: String) = classLoader.loadClass(classOf[Password].getName)
                                             .getConstructor(classOf[String])
                                             .newInstance(value).asInstanceOf[Object]
    props.asInstanceOf[Map[String, Object]].map {
      case (key, clazz: Class[_]) => (key, classPropValue(clazz))
      case (key, list: java.util.List[_]) => (key,
          if (!list.isEmpty && list.get(0).isInstanceOf[Class[_]])
            list.map(clazz => classPropValue(clazz.asInstanceOf[Class[_]])).asJava
          else
            list)
      case (key, value: Password) => (key, password(value.value))
      case (key, value) => (key, value)
    }.asJava
  }

  class TestClassLoader(urls: Array[URL] = classOf[TestClassLoader].getClassLoader.asInstanceOf[URLClassLoader].getURLs, delegate: URLClassLoader = null) extends URLClassLoader(urls, null) {
    classLoaders.add(this)

    override def findClass(name: String) : Class[_] = {
      if (delegate != null && name.startsWith("org.apache.kafka"))
        delegate.loadClass(name)
      else
        super.findClass(name)
    }
  }
}

class TestSerializer extends StringSerializer
class TestDeserializer extends StringDeserializer
class TestProducerInterceptor extends MockProducerInterceptor
class TestConsumerInterceptor extends MockConsumerInterceptor
class TestPrincipalBuilder extends DefaultPrincipalBuilder
class TestPartitioner extends DefaultPartitioner
class TestPartitionAssignor extends RoundRobinAssignor

class TestProducer(producerProps: Map[String, Object], topic: String, numRecords: Int) extends Runnable with Closeable {

  val producer = new KafkaProducer[String, String](producerProps)

  override def run() {
    var numBytesProduced = 0
    for (i <- 0 until numRecords) {
      val payload = i.toString
      numBytesProduced += payload.length
      producer.send(new ProducerRecord[String, String](topic, null, null, payload)).get
    }
    if (producerProps.containsKey(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG))
      assertEquals(numRecords, MockProducerInterceptor.ON_SUCCESS_COUNT.get)
  }

  override def close() {
    producer.close()
  }

}

class TestConsumer(consumerProps: Map[String, Object], topic: String, numRecords: Int) extends Runnable with Closeable {
  val consumer = new KafkaConsumer(consumerProps)

  override def run() {
    val endTimeMs = System.currentTimeMillis + 10000
    consumer.subscribe(List(topic))
    var numConsumed = 0
    while (numConsumed < numRecords && System.currentTimeMillis < endTimeMs) {
      for (cr <- consumer.poll(100)) {
        numConsumed += 1
        consumer.commitSync()
      }
    }
    assertEquals(numRecords, numConsumed)
    if (consumerProps.containsKey(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG))
      assertEquals(numRecords, MockConsumerInterceptor.ON_COMMIT_COUNT.get)
  }

  override def close() {
    consumer.close()
  }
}
