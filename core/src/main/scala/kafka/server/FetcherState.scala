/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

sealed abstract class FetcherState {
  def value: Byte

  def rateAndTimeMetricName: Option[String] =
    if (hasRateAndTimeMetric) Some(s"${toString}RateAndTimeMs") else None

  protected def hasRateAndTimeMetric: Boolean = true
}

object FetcherState {
  case object Idle extends FetcherState {
    def value = 0
    override protected def hasRateAndTimeMetric: Boolean = false
  }

  case object AddPartitions extends FetcherState {
    def value = 1
  }

  case object RemovePartitions extends FetcherState {
    def value = 2
  }

  case object GetPartitionCount extends FetcherState {
    def value = 3
  }

  case object TruncateAndFetch extends FetcherState {
    def value = 4
  }

  val values: Seq[FetcherState] = Seq(Idle, AddPartitions, RemovePartitions, GetPartitionCount, TruncateAndFetch)
}
