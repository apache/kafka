package kafka.security.audit

import java.util
import java.util.concurrent.{Executors, TimeUnit}
import java.util.UUID

import kafka.utils.Logging
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.server.auditor.{AuditEvent, AuditInfo, Auditor}
import org.apache.kafka.server.auditor.events.{AclEvent, ConfigsEvent, DeleteOffsetsEvent, DeleteRecordsEvent, LogDirEvent, ProduceEvent, SyncGroupEvent, TopicEvent}
import org.apache.kafka.server.authorizer.AuthorizableRequestContext

import scala.jdk.CollectionConverters._

class LoggingAuditor extends Auditor with Logging {

  private val producerCache = new ClientRelationshipCache(10000)
  private val consumerCache = new ClientRelationshipCache(10000)
  private val executor = Executors.newSingleThreadExecutor()
  private val logLineFormat = "    %-25s%-25s\n"

  /**
   * Called on request completion before returning the response to the client. It allows auditing multiple resources
   * in the request, such as multiple topics being created.
   *
   * @param event          is the request specific data passed down to the auditor.
   * @param requestContext contains metadata to the request.
   */
  override def audit(event: AuditEvent, requestContext: AuthorizableRequestContext): Unit = executor.execute(() => {
    event match {
      case e: ProduceEvent => logProduceAudit(e, requestContext)
      case e: SyncGroupEvent => logSyncGroupAudit(e, requestContext)
      case e: TopicEvent => logTopicAudit(e, requestContext)
      case e: AclEvent[_,_] => logAclEvent(e, requestContext)
      case e: LogDirEvent => logLogDirEvent(e, requestContext)
      case e: ConfigsEvent => logConfigsEvent(e, requestContext)
      case e: DeleteRecordsEvent => logDeleteRecordsEvent(e, requestContext)
      case e: DeleteOffsetsEvent => logDeleteOffsetEvent(e, requestContext)
    }
  })


  override def close(): Unit = if (executor != null) {
    executor.shutdown()
    executor.awaitTermination(60, TimeUnit.SECONDS)
  }

  /**
   * Configure this class with the given key-value pairs
   */
  override def configure(configs: util.Map[String, _]): Unit = { }

  private def logTopicAudit(event: TopicEvent,
                            ctx: AuthorizableRequestContext): Unit = event.auditInfo.asScala.foreach { case (topic, ai) =>
    val eventStr = event.eventType match {
      case TopicEvent.EventType.CREATE => "create"
      case TopicEvent.EventType.DELETE => "delete"
    }
    val eventLogInfo = Map(
      "topic:" -> topic.name
    )
    info(s"Auditing topic $eventStr", eventLogInfo, ctx, ai, event.id)
  }

  private def logProduceAudit(event: ProduceEvent,
                              ctx: AuthorizableRequestContext): Unit =
    event.topicAuditInfo.asScala.foreach { case (topicPartition, auditInfo) =>
      if (producerCache.putRelationship(event.clientId, topicPartition.topic)) {
        val eventLogInfo = Map(
          "clientId:" -> event.clientId,
          "topic:" -> topicPartition.topic
        )
        info("Auditing new producer", eventLogInfo, ctx, auditInfo, event.id)
      }
  }

  private def logSyncGroupAudit(event: SyncGroupEvent,
                                ctx: AuthorizableRequestContext): Unit = {
    val newTopics = event.assignedTopics.asScala.filter(topic => consumerCache.putRelationship(event.clientId, topic))
    val eventLogInfo = Map(
      "clientId:" -> event.clientId,
      "groupId:" -> event.groupId,
      "topics:" -> String.join(",", newTopics.asJava)
    )
    info("Auditing new consumer", eventLogInfo, ctx, event.groupIdAuditInfo, event.id)
  }

  private def logAclEvent(event: AclEvent[_, _], ctx: AuthorizableRequestContext): Unit =  {
    val eventStr = event.eventType match {
      case AclEvent.EventType.CREATE => "create"
      case AclEvent.EventType.DESCRIBE => "describe"
      case AclEvent.EventType.DELETE => "delete"
    }
    val eventLogInfo = Map(
      "acls:" -> String.join(",", event.auditedEntities.asScala.map(_.toString).asJava)
    )
    info(s"Auditing ACL $eventStr", eventLogInfo, ctx, event.clusterAuditInfo, event.id)
  }

  private def logLogDirEvent(event: LogDirEvent,
                             ctx: AuthorizableRequestContext): Unit = {
    val eventStr = event.eventType match {
      case LogDirEvent.EventType.DESCRIBE => "describe"
      case LogDirEvent.EventType.ALTER => "alter"
    }
    event.topicAuditInfo.asScala.foreach { case (tp, auditInfo) =>
      val eventLogInfo = Map(
        "logDir:" -> event.partitionDirs.get(tp),
        "topic-partition:" -> tp.toString
      )
      info(s"Auditing logDir $eventStr", eventLogInfo, ctx, auditInfo, event.id)
    }
  }

  private def logConfigsEvent(event: ConfigsEvent,
                              ctx: AuthorizableRequestContext): Unit = {
    val eventStr = event.eventType match {
      case ConfigsEvent.EventType.DESCRIBE => "describe"
      case ConfigsEvent.EventType.ALTER => "alter"
      case ConfigsEvent.EventType.INCREMENTAL_ALTER => "incremental alter"
    }
    event.auditedConfigs.asScala.foreach { case (config, auditInfo) =>
      val eventLogInfo = Map(
        "config:" -> config.toString
      )
      info(s"Auditing configs $eventStr", eventLogInfo, ctx, auditInfo, event.id)
    }
  }

  private def logDeleteRecordsEvent(event: DeleteRecordsEvent,
                                    ctx: AuthorizableRequestContext): Unit = {
    event.auditedTopics.asScala.foreach { tp =>
      val eventLogInfo = Map(
        "topic-partition:" -> tp.toString,
        "offset:" -> event.deleteOffset(tp).toString
      )
      info(s"Auditing record deletion", eventLogInfo, ctx, event.auditInfo(tp), event.id)
    }
  }

  private def logDeleteOffsetEvent(event: DeleteOffsetsEvent,
                                   ctx: AuthorizableRequestContext): Unit = {
    val groupEventLogInfo = Map(
      "groupId:" -> event.groupId
    )
    info("Auditing offset delete group", groupEventLogInfo, ctx, event.groupIdAuditInfo, event.id)
    event.partitionsToDelete.asScala.foreach { tp =>
      val topicEventLogInfo = Map(
        "topic-partition:" -> tp.toString
      )
      info("Auditing offset delete group", topicEventLogInfo, ctx, event.topicAuditInfo(tp), event.id)
    }
  }

  private def info(header: String,
                   extraArgs: Map[String, String],
                   ctx: AuthorizableRequestContext,
                   auditAuthInfo: AuditInfo,
                   id: UUID): Unit = {
    val b = new StringBuilder
    b.append(header)
    b.append(" with UUID ")
    b.append(id.toString)
    b.append("\n")
    extraArgs.foreach { case (key, value) =>
      b.append(String.format(logLineFormat, key, value))
    }
    b.append(String.format(logLineFormat, "allowed:", auditAuthInfo.allowed))
    b.append(String.format(logLineFormat, "error:", Errors.forCode(auditAuthInfo.errorCode.toShort)))
    b.append(String.format(logLineFormat, "principal:", ctx.principal.getName))
    b.append(String.format(logLineFormat, "address:", ctx.clientAddress))
    info(b.toString)
  }
}
