package kafka.server

import kafka.Kafka.info
import kafka.metrics.clientmetrics.{ClientMetricsCache, ClientMetricsConfig, CmClientInformation, CmClientInstanceState}
import kafka.network.RequestChannel
import kafka.server.ClientMetricsManager.getSupportedCompressionTypes
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.requests.{GetTelemetrySubscriptionRequest, GetTelemetrySubscriptionResponse}

import java.util.{Calendar, Properties}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object ClientMetricsManager {
  private val _instance = new ClientMetricsManager
  def getInstance = _instance

  def getSupportedCompressionTypes: List[java.lang.Byte] = {
    val compressionTypes = new ListBuffer[java.lang.Byte]
    CompressionType.values.foreach(x => compressionTypes.append(x.id.toByte))
    compressionTypes.toList
  }

  def processGetTelemetrySubscriptionRequest(request: RequestChannel.Request,
                                             throttleMs: Int): GetTelemetrySubscriptionResponse = {
    val subscriptionRequest = request.body[GetTelemetrySubscriptionRequest]
    val clientInfo = CmClientInformation(request, subscriptionRequest.getClientInstanceId.toString)
    _instance.processGetSubscriptionRequest(subscriptionRequest, clientInfo, throttleMs)
  }

}

class ClientMetricsManager {
  def getClientInstance(id: Uuid) = ClientMetricsCache.getInstance.get(id)

  def processGetSubscriptionRequest(subscriptionRequest: GetTelemetrySubscriptionRequest,
                                    clientInfo: CmClientInformation,
                                    throttleMs: Int): GetTelemetrySubscriptionResponse = {
    var clientInstanceId = subscriptionRequest.getClientInstanceId
    if (clientInstanceId == null || clientInstanceId == Uuid.ZERO_UUID) {
      clientInstanceId = Uuid.randomUuid()
    }
    var clientInstance = getClientInstance(clientInstanceId)
    if (clientInstance == null) {
      clientInstance = createClientInstance(clientInstanceId, clientInfo)
    }

    val data =  new GetTelemetrySubscriptionsResponseData()
        .setThrottleTimeMs(throttleMs)
        .setClientInstanceId(clientInstanceId)
        .setSubscriptionId(clientInstance.getSubscriptionId) // TODO: should we use LONG instead of int?
        .setAcceptedCompressionTypes(getSupportedCompressionTypes.asJava)
        .setPushIntervalMs(clientInstance.getPushIntervalMs)
        .setDeltaTemporality(true)
        .setErrorCode(Errors.NONE.code())
        .setRequestedMetrics(clientInstance.getMetrics.asJava)

    if (clientInstance.isDisabledForMetricsCollection) {
      info(s"Metrics collection is disabled for the client: ${clientInstance.getId.toString}")
    }

    clientInstance.updateLastAccessTs(Calendar.getInstance.getTime.getTime)

    new GetTelemetrySubscriptionResponse(data)
  }

  def updateSubscription(groupId :String, properties :Properties) = {
    ClientMetricsConfig.updateClientSubscription(groupId, properties)
  }

  def createClientInstance(clientInstanceId: Uuid, clientInfo: CmClientInformation): CmClientInstanceState = {
    val clientInstance = CmClientInstanceState(clientInstanceId, clientInfo,
                                               ClientMetricsConfig.getClientSubscriptions)
    // Add to the cache and if cache size > max entries then time to make some room by running
    // GC to clean up all the expired entries in the cache.
    ClientMetricsCache.getInstance.add(clientInstance)
    ClientMetricsCache.runGCIfNeeded()
    clientInstance
  }

}

