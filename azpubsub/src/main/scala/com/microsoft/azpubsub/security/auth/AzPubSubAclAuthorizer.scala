package com.microsoft.azpubsub.security.auth

import java.util.concurrent._

import scala.collection.JavaConverters.asScalaSetConverter

import com.yammer.metrics.core.{Meter, MetricName}

import org.apache.kafka.common.security.auth.KafkaPrincipal

import kafka.metrics.KafkaMetricsGroup
import kafka.network.RequestChannel.Session
import kafka.security.auth.Operation
import kafka.security.auth.Resource
import kafka.security.auth.SimpleAclAuthorizer
import kafka.utils.Logging

/*
 * AzPubSub ACL Authorizer to handle the certificate & role based principal type
 */
class AzPubSubAclAuthorizer extends SimpleAclAuthorizer with Logging with KafkaMetricsGroup {
  override def metricName(name: String, metricTags: scala.collection.Map[String, String]): MetricName = {
    explicitMetricName("azpubsub.security", "AuthorizerMetrics", name, metricTags)
  }

  private val successRate: Meter = newMeter("AuthorizerSuccessPerSec", "success", TimeUnit.SECONDS)
  private val failureRate: Meter = newMeter("AuthorizerFailurePerSec", "failure", TimeUnit.SECONDS)

  override def authorize(session: Session, operation: Operation, resource: Resource): Boolean = {
    val sessionPrincipal = session.principal
    if (classOf[AzPubSubPrincipal] == sessionPrincipal.getClass) {
      val principal = sessionPrincipal.asInstanceOf[AzPubSubPrincipal]
      for (role <- principal.getRoles.asScala) {
        val claimPrincipal = new KafkaPrincipal(principal.getPrincipalType(), role)
        val claimSession = new Session(claimPrincipal, session.clientAddress)
        if (super.authorize(claimSession, operation, resource)) {
          successRate.mark()
          return true
        }
      }
    } else if (super.authorize(session, operation, resource)) {
      successRate.mark()
      return true
    }

    failureRate.mark()
    return false
  }
}
