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

package kafka.zk.migration


import kafka.utils.Logging
import kafka.zk.ZkMigrationClient.wrapZkException
import kafka.zk._
import kafka.zookeeper._
import org.apache.kafka.metadata.migration.{DelegationTokenMigrationClient, ZkMigrationLeadershipState}
import org.apache.kafka.common.security.token.delegation.TokenInformation
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.{CreateMode, KeeperException}

import scala.jdk.CollectionConverters._
import scala.collection._

class ZkDelegationTokenMigrationClient(
  zkClient: KafkaZkClient
) extends DelegationTokenMigrationClient with Logging {

  val adminZkClient = new AdminZkClient(zkClient)

  override def getDelegationTokens(): java.util.List[String] = {
      zkClient.getChildren(DelegationTokensZNode.path).asJava
  }

  override def writeDelegationToken(
    tokenId: String,
    tokenInformation: TokenInformation,
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {

    val path = DelegationTokenInfoZNode.path(tokenId)

    def set(tokenData: Array[Byte]): (Int, Seq[SetDataResponse]) = {
      val setRequest = SetDataRequest(path, tokenData, ZkVersion.MatchAnyVersion)
      zkClient.retryMigrationRequestsUntilConnected(Seq(setRequest), state)
    }

    def create(tokenData: Array[Byte]): (Int, Seq[CreateResponse]) = {
      val createRequest = CreateRequest(path, tokenData, zkClient.defaultAcls(path), CreateMode.PERSISTENT)
      zkClient.retryMigrationRequestsUntilConnected(Seq(createRequest), state)
    }

    val tokenInfo = DelegationTokenInfoZNode.encode(tokenInformation)
    val (setMigrationZkVersion, setResponses) = set(tokenInfo)
    if (setResponses.head.resultCode.equals(Code.NONODE)) {
      val (createMigrationZkVersion, createResponses) = create(tokenInfo)
      if (createResponses.head.resultCode.equals(Code.OK)) {
        state.withMigrationZkVersion(createMigrationZkVersion)
      } else {
        throw KeeperException.create(createResponses.head.resultCode, path)
      }
    } else if (setResponses.head.resultCode.equals(Code.OK)) {
      state.withMigrationZkVersion(setMigrationZkVersion)
    } else {
      throw KeeperException.create(setResponses.head.resultCode, path)
    }
  }

  override def deleteDelegationToken(
    tokenId: String,
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {

    val path = DelegationTokenInfoZNode.path(tokenId)
    val requests = Seq(DeleteRequest(path, ZkVersion.MatchAnyVersion))
    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests, state)

    if (responses.head.resultCode.equals(Code.NONODE)) {
      // Not fatal.
      error(s"Did not delete $tokenId since the node did not exist.")
      state
    } else if (responses.head.resultCode.equals(Code.OK)) {
      state.withMigrationZkVersion(migrationZkVersion)
    } else {
      throw KeeperException.create(responses.head.resultCode, path)
    }
  }
}

