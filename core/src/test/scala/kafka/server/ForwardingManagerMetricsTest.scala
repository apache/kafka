package kafka.server

import com.yammer.metrics.core.{Gauge, Histogram}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._
import scala.util.Using

final class ForwardingManagerMetricsTest {
  @Test
  def testMetricsNames(): Unit = {
    val metricsRegistry = KafkaYammerMetrics.defaultRegistry()

    val expectedMetrics = Set(
      "kafka.server:type=ForwardingManager,name=QueueTimeMs",
      "kafka.server:type=ForwardingManager,name=QueueLength",
      "kafka.server:type=ForwardingManager,name=RemoteTimeMs"
    )

    Using(new ForwardingManagerMetrics) { _ =>
      val histogramsMap = metricsRegistry.allMetrics().asScala.filter { case (name, _) => name.getGroup == "kafka.server" && name.getType == "ForwardingManager"}
      assertEquals(histogramsMap.size, expectedMetrics.size)
      histogramsMap.foreach {case (name, _) =>
        assertTrue(expectedMetrics.contains(name.getMBeanName))
      }
    }

    val histogramsMap = metricsRegistry.allMetrics().asScala.filter { case (name, _) => name.getGroup == "kafka.server" && name.getType == "ForwardingManager" }
    assertEquals(0, histogramsMap.size)
  }

  @Test
  def testQueueLength(): Unit = {
    val metricsRegistry = KafkaYammerMetrics.defaultRegistry()

    Using(new ForwardingManagerMetrics) { forwardingManagerMetrics =>
      val queueLength = metricsRegistry.allMetrics().get(forwardingManagerMetrics.queueLengthName).asInstanceOf[Gauge[Int]]
      assertEquals(0, queueLength.value())
      forwardingManagerMetrics.queueLength.getAndIncrement()
      assertEquals(1, queueLength.value())
    }
  }

  @Test
  def testQueueTimeMs(): Unit = {
    val metricsRegistry = KafkaYammerMetrics.defaultRegistry()

    Using(new ForwardingManagerMetrics) { forwardingManagerMetrics =>
      val queueTimeMs = metricsRegistry.allMetrics().get(forwardingManagerMetrics.queueTimeMsName).asInstanceOf[Histogram]
      forwardingManagerMetrics.queueTimeMsHist.update(10)
      assertEquals(10, queueTimeMs.max())
      forwardingManagerMetrics.queueTimeMsHist.update(20)
      assertEquals(15, queueTimeMs.mean().asInstanceOf[Long])
    }
  }

  @Test
  def testRemoteTimeMs(): Unit = {
    val metricsRegistry = KafkaYammerMetrics.defaultRegistry()

    Using(new ForwardingManagerMetrics) { forwardingManagerMetrics =>
      val remoteTimeMs = metricsRegistry.allMetrics().get(forwardingManagerMetrics.remoteTimeMsName).asInstanceOf[Histogram]
      forwardingManagerMetrics.remoteTimeMsHist.update(10)
      assertEquals(10, remoteTimeMs.max())
      forwardingManagerMetrics.remoteTimeMsHist.update(20)
      assertEquals(15, remoteTimeMs.mean().asInstanceOf[Long])
    }
  }

}
