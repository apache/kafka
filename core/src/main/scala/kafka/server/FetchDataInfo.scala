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

import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction

sealed trait FetchIsolation
case object FetchLogEnd extends FetchIsolation
case object FetchHighWatermark extends FetchIsolation
case object FetchTxnCommitted extends FetchIsolation

case class FetchDataInfo(fetchOffsetMetadata: LogOffsetMetadata,
                         records: Records,
                         firstEntryIncomplete: Boolean = false,
                         abortedTransactions: Option[List[AbortedTransaction]] = None)
