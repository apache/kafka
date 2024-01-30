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
package kafka.zk.migration

import kafka.server.{KafkaConfig, QuorumTestHarness}
import kafka.zk.ZkMigrationClient
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.migration.ZkMigrationLeadershipState
import org.apache.kafka.security.PasswordEncoder
import org.junit.jupiter.api.{BeforeEach, TestInfo}

import java.util.Properties

class ZkMigrationTestHarness extends QuorumTestHarness {
  val InitialControllerEpoch: Int = 42

  val InitialKRaftEpoch: Int = 0

  var migrationClient: ZkMigrationClient = _

  var migrationState: ZkMigrationLeadershipState = _

  val SECRET = "secret"

  val NEW_SECRET = "newSecret"

  val encoder: PasswordEncoder = {
    val encoderProps = new Properties()
    encoderProps.put(KafkaConfig.ZkConnectProp, "localhost:1234") // Get around the config validation
    encoderProps.put(KafkaConfig.PasswordEncoderSecretProp, SECRET) // Zk secret to encrypt the
    val encoderConfig = new KafkaConfig(encoderProps)
    PasswordEncoder.encrypting(encoderConfig.passwordEncoderSecret.get,
      encoderConfig.passwordEncoderKeyFactoryAlgorithm,
      encoderConfig.passwordEncoderCipherAlgorithm,
      encoderConfig.passwordEncoderKeyLength,
      encoderConfig.passwordEncoderIterations)
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    zkClient.createControllerEpochRaw(1)
    migrationClient = ZkMigrationClient(zkClient, encoder)
    migrationState = initialMigrationState
    migrationState = migrationClient.getOrCreateMigrationRecoveryState(migrationState)
  }

  private def initialMigrationState: ZkMigrationLeadershipState = {
    val (epoch, stat) = zkClient.getControllerEpoch.get
    new ZkMigrationLeadershipState(3000, InitialControllerEpoch, 100, InitialKRaftEpoch, Time.SYSTEM.milliseconds(), -1, epoch, stat.getVersion)
  }
}
