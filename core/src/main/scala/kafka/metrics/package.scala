package kafka

import javax.management.{MalformedObjectNameException, ObjectName}

import com.codahale.metrics.jmx.{JmxReporter, ObjectNameFactory}
import com.codahale.metrics.{Metric, MetricRegistry}

import scala.collection.JavaConverters._

package object metrics {

  private[metrics] val kafkaMetricRegistry = new MetricRegistry()

  private val objectNameFactory = new ObjectNameFactory {
    override def createName(typeName: String, domain: String, name: String): ObjectName =
      try {
        new ObjectName(name)
      } catch {
        case _: MalformedObjectNameException => new ObjectName(ObjectName.quote(name))
      }
  }

  private val jmxReporter = JmxReporter.forRegistry(kafkaMetricRegistry).createsObjectNamesWith(objectNameFactory).build()
  jmxReporter.start()

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() {
      jmxReporter.stop()
    }
  })

  def getKafkaMetrics(): Map[String, Metric] = kafkaMetricRegistry.getMetrics().asScala.toMap

}
