/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server.epoch

import org.apache.kafka.server.common.MetadataVersion.IBP_2_6_IV0

/**
 * With IBP 2.7 onwards, we truncate based on diverging epochs returned in fetch responses.
 * EpochDrivenReplicationProtocolAcceptanceTest tests epochs with latest version. This test
 * verifies that we handle older IBP versions with truncation on leader/follower change correctly.
 */
class EpochDrivenReplicationProtocolAcceptanceWithIbp26Test extends EpochDrivenReplicationProtocolAcceptanceTest {
  override val metadataVersion = IBP_2_6_IV0
}
