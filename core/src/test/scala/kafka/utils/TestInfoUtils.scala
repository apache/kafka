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

import scala.compat.java8.OptionConverters._

class EmptyTestInfo extends TestInfo {
  override def getDisplayName: String = ""
  override def getTags: util.Set[String] = Collections.emptySet()
  override def getTestClass: (Optional[Class[_]]) = Optional.empty()
  override def getTestMethod: Optional[Method] = Optional.empty()
}

object TestInfoUtils {
  val legacyTestClassNames = Set(
    "integration.kafka.network.DynamicNumNetworkThreadsTest",
    "integration.kafka.server.FetchRequestBetweenDifferentIbpTest",
    "integration.kafka.server.FetchRequestTestDowngrade",
    "kafka.admin.AclCommandTest",
    "kafka.admin.AddPartitionsTest",
    "kafka.admin.AdminZkClientTest",
    "kafka.admin.BrokerApiVersionsCommandTest",
    "kafka.admin.DelegationTokenCommandTest",
    "kafka.admin.DeleteConsumerGroupsTest",
    "kafka.admin.DeleteOffsetsConsumerGroupCommandIntegrationTest",
    "kafka.admin.DeleteTopicTest",
    "kafka.admin.DescribeConsumerGroupTest",
    "kafka.admin.FeatureCommandTest",
    "kafka.admin.ListConsumerGroupTest",
    "kafka.admin.ListOffsetsIntegrationTest",
    "kafka.admin.LogDirsCommandTest",
    "kafka.admin.ResetConsumerGroupOffsetTest",
    "kafka.admin.UserScramCredentialsCommandTest",
    "kafka.api.ConsumerBounceTest",
    "kafka.api.ConsumerWithLegacyMessageFormatIntegrationTest",
    "kafka.api.CustomQuotaCallbackTest",
    "kafka.api.DelegationTokenEndToEndAuthorizationTest",
    "kafka.api.DelegationTokenEndToEndAuthorizationWithOwnerTest",
    "kafka.api.DescribeAuthorizedOperationsTest",
    "kafka.api.GroupCoordinatorIntegrationTest",
    "kafka.api.GroupEndToEndAuthorizationTest",
    "kafka.api.MetricsTest",
    "kafka.api.PlaintextAdminIntegrationTest",
    "kafka.api.PlaintextConsumerTest",
    "kafka.api.PlaintextEndToEndAuthorizationTest",
    "kafka.api.PlaintextEndToEndAuthorizerationTest",
    "kafka.api.PlaintextProducerSendTest",
    "kafka.api.ProducerFailureHandlingTest",
    "kafka.api.ProducerSendWhileDeletionTest",
    "kafka.api.RackAwareAutoTopicCreationTest",
    "kafka.api.SaslClientsWithInvalidCredentialsTest",
    "kafka.api.SaslGssapiSslEndToEndAuthorizationTest",
    "kafka.api.SaslMultiMechanismConsumerTest",
    "kafka.api.SaslOAuthBearerSslEndToEndAuthorizationTest",
    "kafka.api.SaslPlainPlaintextConsumerTest",
    "kafka.api.SaslPlainSslEndToEndAuthorizationTest",
    "kafka.api.SaslPlaintextConsumerTest",
    "kafka.api.SaslScramSslEndToEndAuthorizationTest",
    "kafka.api.SaslSslAdminIntegrationTest",
    "kafka.api.SaslSslConsumerTest",
    "kafka.api.SslAdminIntegrationTest",
    "kafka.api.SslConsumerTest",
    "kafka.api.SslEndToEndAuthorizationTest",
    "kafka.api.SslProducerSendTest",
    "kafka.api.TransactionsBounceTest",
    "kafka.api.TransactionsWithMaxInFlightOneTest",
    "kafka.api.test.ProducerCompressionTest",
    "kafka.common.ZkNodeChangeNotificationListenerTest",
    "kafka.controller.ControllerFailoverTest",
    "kafka.controller.ControllerIntegrationTest",
    "kafka.integration.MetricsDuringTopicCreationDeletionTest",
    "kafka.integration.UncleanLeaderElectionTest",
    "kafka.network.DynamicConnectionQuotaTest",
    "kafka.security.auth.ZkAuthorizationTest",
    "kafka.security.authorizer.AclAuthorizerTest",
    "kafka.security.authorizer.AclAuthorizerWithZkSaslTest",
    "kafka.security.authorizer.AuthorizerInterfaceDefaultTest",
    "kafka.security.token.delegation.DelegationTokenManagerTest",
    "kafka.server.AddPartitionsToTxnRequestServerTest",
    "kafka.server.AdvertiseBrokerTest",
    "kafka.server.AlterReplicaLogDirsRequestTest",
    "kafka.server.AlterUserScramCredentialsRequestNotAuthorizedTest",
    "kafka.server.AlterUserScramCredentialsRequestTest",
    "kafka.server.BrokerEpochIntegrationTest",
    "kafka.server.ControllerMutationQuotaTest",
    "kafka.server.DelegationTokenRequestsOnPlainTextTest",
    "kafka.server.DelegationTokenRequestsTest",
    "kafka.server.DelegationTokenRequestsWithDisableTokenFeatureTest",
    "kafka.server.DeleteTopicsRequestWithDeletionDisabledTest",
    "kafka.server.DescribeClusterRequestTest",
    "kafka.server.DescribeLogDirsRequestTest",
    "kafka.server.DescribeUserScramCredentialsRequestNotAuthorizedTest",
    "kafka.server.DescribeUserScramCredentialsRequestTest",
    "kafka.server.DynamicBrokerReconfigurationTest",
    "kafka.server.DynamicConfigTest",
    "kafka.server.EdgeCaseRequestTest",
    "kafka.server.FetchRequestDownConversionConfigTest",
    "kafka.server.FetchRequestMaxBytesTest",
    "kafka.server.FetchRequestTest",
    "kafka.server.FetchRequestWithLegacyMessageFormatTest",
    "kafka.server.FinalizedFeatureChangeListenerTest",
    "kafka.server.GssapiAuthenticationTest",
    "kafka.server.KafkaMetricReporterClusterIdTest",
    "kafka.server.KafkaMetricReporterExceptionHandlingTest",
    "kafka.server.KafkaMetricsReporterTest",
    "kafka.server.KafkaServerTest",
    "kafka.server.LeaderElectionTest",
    "kafka.server.ListOffsetsRequestTest",
    "kafka.server.LogDirFailureTest",
    "kafka.server.LogRecoveryTest",
    "kafka.server.MetadataRequestBetweenDifferentIbpTest",
    "kafka.server.MultipleListenersWithAdditionalJaasContextTest",
    "kafka.server.MultipleListenersWithDefaultJaasContextTest",
    "kafka.server.OffsetsForLeaderEpochRequestTest",
    "kafka.server.ProduceRequestTest",
    "kafka.server.ReplicationQuotasTest",
    "kafka.server.RequestQuotaTest",
    "kafka.server.ScramServerStartupTest",
    "kafka.server.ServerGenerateClusterIdTest",
    "kafka.server.ServerShutdownTest",
    "kafka.server.ServerStartupTest",
    "kafka.server.StopReplicaRequestTest",
    "kafka.server.TopicIdWithOldInterBrokerProtocolTest",
    "kafka.server.UpdateFeaturesTest",
    "kafka.server.epoch.EpochDrivenReplicationProtocolAcceptanceTest",
    "kafka.server.epoch.EpochDrivenReplicationProtocolAcceptanceWithIbp26Test",
    "kafka.server.epoch.LeaderEpochIntegrationTest",
    "kafka.tools.GetOffsetShellTest",
    "kafka.tools.MirrorMakerIntegrationTest",
    "kafka.utils.ReplicationUtilsTest",
    "kafka.zk.KafkaZkClientTest",
    "kafka.zookeeper.ZooKeeperClientTest",
  )

  def isKRaft(testInfo: TestInfo): Boolean = {
    if (testInfo.getDisplayName().contains("quorum=")) {
      if (testInfo.getDisplayName().contains("quorum=kraft")) {
        true
      } else if (testInfo.getDisplayName().contains("quorum=zk")) {
        false
      } else {
        throw new RuntimeException(s"Unknown quorum value")
      }
    } else {
      // If there is no quorum parameter, we check if any class in our current stack trace is a legacy test class.
      // If it is, we default to ZK mode. Otherwise, we default to KRaft mode.
      testInfo.getTestClass().asScala match {
        case None => false
        case Some(testClass) => !legacyTestClassNames.contains(testClass.getCanonicalName)
      }
    }
  }

  final val TestWithParameterizedQuorumName = "{displayName}.quorum={0}"
}
