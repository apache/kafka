package integration.kafka.api

import org.apache.kafka.common.metrics.{KafkaMetric, MetricsReporter}
import org.apache.kafka.server.authorizer.AuthorizableRequestContext
import org.apache.kafka.server.telemetry.{ClientTelemetry, ClientTelemetryPayload, ClientTelemetryReceiver}

import java.util

class MyClientTelemetry extends ClientTelemetry with MetricsReporter {

  override def clientReceiver(): ClientTelemetryReceiver = {
    (context: AuthorizableRequestContext, payload: ClientTelemetryPayload) => {
      println(payload.data().array().mkString("Array(", ", ", ")"))
    }
    
  }

  override def init(metrics: util.List[KafkaMetric]): Unit = {
    println(metrics)
  }

  override def metricChange(metric: KafkaMetric): Unit = {}

  override def metricRemoval(metric: KafkaMetric): Unit = {}

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {
    println("Configuring MyClientTelemetry")
    println(configs)
  }
}
