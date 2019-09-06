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

package kafka.common

import java.util.Optional

case class OffsetAndMetadata(offset: Long,
                             leaderEpoch: Optional[Integer],
                             metadata: String,
                             commitTimestamp: Long,
                             expireTimestamp: Option[Long]) {


  override def toString: String  = {
    s"OffsetAndMetadata(offset=$offset" +
      s", leaderEpoch=$leaderEpoch" +
      s", metadata=$metadata" +
      s", commitTimestamp=$commitTimestamp" +
      s", expireTimestamp=$expireTimestamp)"
  }
}

object OffsetAndMetadata {
  val NoMetadata: String = ""

  def apply(offset: Long, metadata: String, commitTimestamp: Long): OffsetAndMetadata = {
    OffsetAndMetadata(offset, Optional.empty(), metadata, commitTimestamp, None)
  }

  def apply(offset: Long, metadata: String, commitTimestamp: Long, expireTimestamp: Long): OffsetAndMetadata = {
    OffsetAndMetadata(offset, Optional.empty(), metadata, commitTimestamp, Some(expireTimestamp))
  }

  def apply(offset: Long, leaderEpoch: Optional[Integer], metadata: String, commitTimestamp: Long): OffsetAndMetadata = {
    OffsetAndMetadata(offset, leaderEpoch, metadata, commitTimestamp, None)
  }
}
