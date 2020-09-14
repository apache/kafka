package com.microsoft.azpubsub.security;

import com.yammer.metrics.core.MetricName
import kafka.metrics.KafkaMetricsGroup

class AzPubSubSecurityMetrics extends KafkaMetricsGroup {
  override def metricName(name: String, metricTags: scala.collection.Map[String, String]): MetricName = {
    explicitMetricName("azpubsub.security", "AuthenticatorMetrics", name, metricTags)
  }
}