/**
 * Licensed under the Apache License, Version 2.0 (the "License");
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
 **/

package kafka.server

import java.net.Socket
import java.util.Properties

import kafka.utils.TestUtils
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.{ListGroupsRequest,ListGroupsResponse}
import org.apache.kafka.common.metrics.MetricsReporter
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.protocol.Errors

import org.junit.Assert._
import org.junit.{Before, Test}
import org.junit.After
import java.util.concurrent.atomic.AtomicInteger

/*
 * this test checks that a reporter that throws an exception will not affect other reporters
 * and will not affect the broker's message handling
 */
class KafkaMetricReporterExceptionHandlingTest extends BaseRequestTest {

  override def numBrokers: Int = 1

  override def propertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.MetricReporterClassesProp, classOf[KafkaMetricReporterExceptionHandlingTest.BadReporter].getName + "," + classOf[KafkaMetricReporterExceptionHandlingTest.GoodReporter].getName)
  }

  @Before
  override def setUp() {
    super.setUp()

    // need a quota prop to register a "throttle-time" metrics after server startup
    val quotaProps = new Properties()
    quotaProps.put(DynamicConfig.Client.RequestPercentageOverrideProp, "0.1")
    adminZkClient.changeClientIdConfig("<default>", quotaProps)
  }

  @After
  override def tearDown() {
    KafkaMetricReporterExceptionHandlingTest.goodReporterRegistered.set(0)
    KafkaMetricReporterExceptionHandlingTest.badReporterRegistered.set(0)
    
    super.tearDown()
  }

  @Test
  def testBothReportersAreInvoked() {
    val port = anySocketServer.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    val socket = new Socket("localhost", port)
    socket.setSoTimeout(10000)

    try {
      TestUtils.retry(10000) {
        val error = new ListGroupsResponse(requestResponse(socket, "clientId", 0, new ListGroupsRequest.Builder())).error()
        assertEquals(Errors.NONE, error)
        assertEquals(KafkaMetricReporterExceptionHandlingTest.goodReporterRegistered.get, KafkaMetricReporterExceptionHandlingTest.badReporterRegistered.get)
        assertTrue(KafkaMetricReporterExceptionHandlingTest.goodReporterRegistered.get > 0)
      }
    } finally {
      socket.close()
    }
  }
}

object KafkaMetricReporterExceptionHandlingTest {
  var goodReporterRegistered = new AtomicInteger
  var badReporterRegistered = new AtomicInteger

  class GoodReporter extends MetricsReporter {

    def configure(configs: java.util.Map[String, _]) {
    }

    def init(metrics: java.util.List[KafkaMetric]) {
    }

    def metricChange(metric: KafkaMetric) {
      if (metric.metricName.group == "Request") {
        goodReporterRegistered.incrementAndGet
      }
    }

    def metricRemoval(metric: KafkaMetric) {
    }

    def close() {
    }
  }

  class BadReporter extends GoodReporter {

    override def metricChange(metric: KafkaMetric) {
      if (metric.metricName.group == "Request") {
        badReporterRegistered.incrementAndGet
        throw new RuntimeException(metric.metricName.toString)
      }
    }
  }
}
