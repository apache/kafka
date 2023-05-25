/**
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

package kafka.controller

import scala.collection.Seq

/**
 * 每类 ControllerState 都定义一个 value 值，表示 Controller 状态的序号，从 0 开始。
 * 另外，rateAndTimeMetricName 方法是用于构造 Controller 状态速率的监控指标名称的。
 *
 * 比如，TopicChange 是一类 ControllerState，用于表示主题总数发生了变化。
 * 为了监控这类状态变更速率，代码中的 rateAndTimeMetricName 方法会定义一个名为 TopicChangeRateAndTimeMs 的指标。
 * 当然，并非所有的 ControllerState 都有对应的速率监控指标，比如，表示空闲状态的 Idle 就没有对应的指标。
 */
sealed abstract class ControllerState {

  def value: Byte

  def rateAndTimeMetricName: Option[String] =
    if (hasRateAndTimeMetric) Some(s"${toString}RateAndTimeMs") else None

  protected def hasRateAndTimeMetric: Boolean = true
}

object ControllerState {

  // Note: `rateAndTimeMetricName` is based on the case object name by default. Changing a name is a breaking change
  // unless `rateAndTimeMetricName` is overridden.

  case object Idle extends ControllerState {
    def value = 0
    override protected def hasRateAndTimeMetric: Boolean = false
  }

  case object ControllerChange extends ControllerState {
    def value = 1
  }

  case object BrokerChange extends ControllerState {
    def value = 2
    // The LeaderElectionRateAndTimeMs metric existed before `ControllerState` was introduced and we keep the name
    // for backwards compatibility. The alternative would be to have the same metric under two different names.
    override def rateAndTimeMetricName = Some("LeaderElectionRateAndTimeMs")
  }

  case object TopicChange extends ControllerState {
    def value = 3
  }

  case object TopicDeletion extends ControllerState {
    def value = 4
  }

  case object AlterPartitionReassignment extends ControllerState {
    def value = 5

    override def rateAndTimeMetricName: Option[String] = Some("PartitionReassignmentRateAndTimeMs")
  }

  case object AutoLeaderBalance extends ControllerState {
    def value = 6
  }

  case object ManualLeaderBalance extends ControllerState {
    def value = 7
  }

  case object ControlledShutdown extends ControllerState {
    def value = 8
  }

  case object IsrChange extends ControllerState {
    def value = 9
  }

  case object LeaderAndIsrResponseReceived extends ControllerState {
    def value = 10
  }

  case object LogDirChange extends ControllerState {
    def value = 11
  }

  case object ControllerShutdown extends ControllerState {
    def value = 12
  }

  case object UncleanLeaderElectionEnable extends ControllerState {
    def value = 13
  }

  case object TopicUncleanLeaderElectionEnable extends ControllerState {
    def value = 14
  }

  case object ListPartitionReassignment extends ControllerState {
    def value = 15
  }

  case object UpdateMetadataResponseReceived extends ControllerState {
    def value = 16

    override protected def hasRateAndTimeMetric: Boolean = false
  }

  case object UpdateFeatures extends ControllerState {
    def value = 17
  }

  val values: Seq[ControllerState] = Seq(Idle, ControllerChange, BrokerChange, TopicChange, TopicDeletion,
    AlterPartitionReassignment, AutoLeaderBalance, ManualLeaderBalance, ControlledShutdown, IsrChange,
    LeaderAndIsrResponseReceived, LogDirChange, ControllerShutdown, UncleanLeaderElectionEnable,
    TopicUncleanLeaderElectionEnable, ListPartitionReassignment, UpdateMetadataResponseReceived,
    UpdateFeatures)
}
