/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.instrumentation

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.Logger
import kafka.cluster.PendingShrinkIsr
import kafka.network.RequestChannel.Request
import kafka.server.instrumentation.ProduceRequestInstrumentation.{Stage, mapper}
import kafka.server.instrumentation.ProduceRequestInstrumentationLogger.Config
import kafka.server.{BrokerReconfigurable, KafkaConfig, ReplicaManager}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.utils.Time

import scala.collection.mutable

object ProduceRequestInstrumentation {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  /**
   * Symbols to be used for marking at each instrumentation stage
   */
  object Stage extends Enumeration {
    // Define stages here
    val Init,
        Authorization,
        BeginAppendRecords,
        AppendToLocalLog,
        ProcessAppendToLocalLogStatus,
        PrepareDelayedProduce,
        EnqueueDelayedProduce,
        BeginResponseCallback,
        ResponseThrottling,
        Finish = Value
  }

  val NoOpProduceRequestInstrumentation = new NoOpProduceRequestInstrumentation()
}

class NoOpProduceRequestInstrumentation extends ProduceRequestInstrumentation(time = null) {
  override def markStage(stage: Stage.Value): Unit = {
  }
}

/**
 * The instrumentation object to be pass along the flow in the produce request
 *
 * @param time The object to obtain timestamp
 */
class ProduceRequestInstrumentation(val time: Time) {
  val marks: mutable.Map[Stage.Value, Long] = mutable.Map()
  var appliedTopicPartitions: Iterable[TopicPartition] = List()

  // Implicitly add init stage on construction
  markStage(ProduceRequestInstrumentation.Stage.Init)

  /**
   * The core of the instrumentation.
   *
   * When the object is passed along, at any point in the code flow, it can invoke this method
   * to mark the beginning of the stage with the current timestamp.
   * The timestamps can later be used to calculate time taken in each stage via a diff on the 2 time stamps
   *
   * @param stage The stage to mark for beginning
   */
  def markStage(stage: Stage.Value): Unit = {
    marks.put(stage, time.milliseconds())
  }
}
object ProduceRequestInstrumentationLogger {
  private val logger = Logger("produce.request.instrumentation.logger")
  /**
   * The config to control the instrumentation behavior
   *
   * @param thresholdToLogMs If the request completed within the threshold, don't log
   * @param logRatio         Ratio of requests to log, applied after all funnels/thresholds
   */
  case class Config(thresholdToLogMs: Long,
                    logRatio: Double)

  val ReconfigurableConfigs: Set[String] = Set(
    KafkaConfig.LiLongTailProduceRequestLogThresholdMsProp,
    KafkaConfig.LiLongTailProduceRequestLogRatioProp,
  )
}

/**
 * Responsible for logging the instrumentation details.
 * The config of the logger is associated with DynamicBrokerConfig lifecycle
 *
 * @param kafkaConfig For configuring dynamic broker config lifecycle
 * @param replicaManager Used to obtain the # of partitions in PendingShrinkIsr
 */
class ProduceRequestInstrumentationLogger(kafkaConfig: KafkaConfig,
                                          time: Time,
                                          rnd: scala.util.Random,
                                          replicaManager: ReplicaManager) extends Logging with BrokerReconfigurable {
  override lazy val logger = ProduceRequestInstrumentationLogger.logger

  // This is a cache of the dynamic config.
  // Should be initialized/reconfigured within the BrokerReconfigurable lifecycle
  validateReconfiguration(kafkaConfig)
  kafkaConfig.dynamicConfig.addBrokerReconfigurable(this)
  var config: Config = extractConfigFromKafkaConfig(kafkaConfig)

  override def reconfigurableConfigs: collection.Set[String] = ProduceRequestInstrumentationLogger.ReconfigurableConfigs

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    val thresholdMs = newConfig.longTailProduceRequestLogThresholdMs
    if (thresholdMs < 0) {
      throw new ConfigException(s"${KafkaConfig.LiLongTailProduceRequestLogThresholdMsProp} should be >= 0")
    }
    val ratio = newConfig.longTailProduceRequestLogRatio
    if (ratio < 0 || ratio > 1) {
      throw new ConfigException(s"${KafkaConfig.LiLongTailProduceRequestLogRatioProp} should be between 0 to 1")
    }
  }

  /**
   * Invoked when the associated DynamicBrokerConfig is changed
   */
  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    config = extractConfigFromKafkaConfig(newConfig)
  }

  def extractConfigFromKafkaConfig(kafkaConfig: KafkaConfig): Config = Config(kafkaConfig.longTailProduceRequestLogThresholdMs,
                                                                              kafkaConfig.longTailProduceRequestLogRatio)

  /**
   * Calculate the time in each stage by
   * 1. Sort stages by timestamp
   * 2. Make a sliding window of 2 on 1.
   * 3. Diff the timestamp on the window to get the time spent on the stage of the first element in 2
   */
  def toTimeTakenInEachStageMessage(instrumentation: ProduceRequestInstrumentation): String = {
    val marks = instrumentation.marks

    // Enforce Finish is marked, so when sliding there is at list a full window (init is force marked on create)
    marks.get(Stage.Finish) match {
      case None => instrumentation.markStage(Stage.Finish)
      case _ =>
    }

    marks
      .toSeq                          // Iterate over stages
      .sortBy(s => (s._2, s._1.id))   // in order of marked timestamp then defined order of stage (to prevent multiple stages finish within 1ms)
      .sliding(2)                     // Make sliding window to diff time taken in each stage
      .map { window =>
        val iter = window.iterator
        val (stage1, timestamp1) = iter.next()
        val (_, timestamp2) = iter.next()
        // Use the next stage's timestamp mark minus this stage's timestamp mark to get time spent on current stage
        // Last stage is on finish, no diff to check, i.e. no need to log
        '\"' + s"${stage1.toString}" + "\":" +  s"${timestamp2 - timestamp1}"
      }
      .mkString("{", ", ", "}")
  }

  /**
   * Based on the config (e.g. probability funnel), logs the detail of the instrumentation
   *
   * @param request Used for capturing detailed info in request
   * @param instrumentation The instrumentation to log
   */
  def maybeLog(request: Request, instrumentation: ProduceRequestInstrumentation): Unit = {

    val totalTimeMs = request.totalTimeMs

    if (totalTimeMs < config.thresholdToLogMs) {
      logger.trace("totalTimeMs={} is below {}={}, skip logging",
                   totalTimeMs,
                   KafkaConfig.LiLongTailProduceRequestLogThresholdMsProp,
                   config.thresholdToLogMs)
      return
    }

    val coin = rnd.nextDouble()
    if (coin > config.logRatio) {
      logger.trace("Funneled out by random coin {} > configured ratio {}", coin, config.logRatio)
      return
    }

    val requestMetrics = Map(
      "totalTimeMs" -> Math.round(request.totalTimeMs),
      "requestQueueTimeMs" -> Math.round(request.requestQueueTimeMs),
      "apiLocalTimeMs" -> Math.round(request.apiLocalTimeMs),
      "apiRemoteTimeMs" -> Math.round(request.apiRemoteTimeMs),
      "apiThrottleTimeMs" -> request.apiThrottleTimeMs,
      "responseQueueTimeMs" -> Math.round(request.responseQueueTimeMs),
      "responseSendTimeMs" -> Math.round(request.responseSendTimeMs),
      "sizeOfBodyInBytes" -> request.sizeOfBodyInBytes.toLong,
      "responseBytes" -> request.responseBytes,
    )

    logger.info(
      s"acks=${request.body[ProduceRequest].acks()}; "
        + s"request_metric=${mapper.writeValueAsString(requestMetrics)}; "
        // instrumentation.appliedTopicPartitions cannot be obtained from request object,
        // because the underlying details, at this moment, have been cleared by ProduceRequest#clearPartitionRecords()
        + s"topic_partitions=${instrumentation.appliedTopicPartitions.map(tp => s"${tp.topic()}-${tp.partition()}").mkString("(", ", ", ")")}; "
        + s"stage_breakdown_ms=${toTimeTakenInEachStageMessage(instrumentation)}; "
        // Shrinking ISR info is important to the produce request.
        // When there are dead followers, an acks=-1 produce may block until the leader discovered it is dead **AND** the full ISR shrinkage to complete.
        + s"${classOf[PendingShrinkIsr].getSimpleName}_count=${replicaManager.numOfPartitionOfIsrState(classOf[PendingShrinkIsr])}"
    )
  }
}
