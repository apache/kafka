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
package kafka.utils

import java.lang.reflect.Method
import java.util
import java.util.{Collections, Optional}

import org.junit.jupiter.api.TestInfo
import org.apache.kafka.clients.consumer.GroupProtocol

class EmptyTestInfo extends TestInfo {
  override def getDisplayName: String = ""
  override def getTags: util.Set[String] = Collections.emptySet()
  override def getTestClass: Optional[Class[_]] = Optional.empty()
  override def getTestMethod: Optional[Method] = Optional.empty()
}

object TestInfoUtils {
  def isKRaft(testInfo: TestInfo): Boolean = {
    if (testInfo.getDisplayName.contains("quorum=")) {
      if (testInfo.getDisplayName.contains("quorum=kraft")) {
        true
      } else if (testInfo.getDisplayName.contains("quorum=zk")) {
        false
      } else {
        throw new RuntimeException(s"Unknown quorum value")
      }
    } else {
      false
    }
  }

  final val TestWithParameterizedQuorumAndGroupProtocolNames = "{displayName}.quorum={0}.groupProtocol={1}"

  def isShareGroupTest(testInfo: TestInfo): Boolean = {
    testInfo.getDisplayName.contains("kraft+kip932")
  }

  def maybeGroupProtocolSpecified(testInfo: TestInfo): Option[GroupProtocol] = {
    if (testInfo.getDisplayName.contains("groupProtocol=classic"))
      Some(GroupProtocol.CLASSIC)
    else if (testInfo.getDisplayName.contains("groupProtocol=consumer"))
      Some(GroupProtocol.CONSUMER)
    else
      None
  }

  /**
   * Returns whether transaction version 2 is enabled.
   * When no parameter is provided, the default returned is true.
   */
  def isTransactionV2Enabled(testInfo: TestInfo): Boolean = {
    !testInfo.getDisplayName.contains("isTV2Enabled=false")
  }

  /**
   * Returns whether eligible leader replicas version 1 is enabled.
   * When no parameter is provided, the default returned is false.
   */
  def isEligibleLeaderReplicasV1Enabled(testInfo: TestInfo): Boolean = {
    testInfo.getDisplayName.contains("isELRV1Enabled=true")
  }
}
