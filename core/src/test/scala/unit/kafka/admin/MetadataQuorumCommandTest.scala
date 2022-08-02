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

package kafka.admin

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterTest, ClusterTestDefaults, ClusterTests, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.utils.TestUtils
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.extension.ExtendWith

import java.util.concurrent.ExecutionException
import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT)
@Tag("integration")
class MetadataQuorumCommandTest(cluster: ClusterInstance) {

  @ClusterTests(
    Array(
      new ClusterTest(clusterType = Type.CO_KRAFT, brokers = 3, controllers = 3),
      new ClusterTest(clusterType = Type.KRAFT, brokers = 3, controllers = 3)
    )
  )
  def testDescribeQuorumSuccessful(): Unit = {
    val initialDescribeOutput = TestUtils.grabConsoleOutput(
      MetadataQuorumCommand.mainNoExit(Array("--bootstrap-server", cluster.bootstrapServers(), "describe"))
    )

    val leaderInfo = initialDescribeOutput.substring(0, initialDescribeOutput.indexOf("voters info:")).trim
    assertEquals(
      1,
      cluster.controllerServers().asScala.map(_.config.nodeId).map(id => s"leaderId: $id").count(info => info == leaderInfo)
    )

    val votersInfo = initialDescribeOutput.substring(initialDescribeOutput.indexOf("voters info:") + 13, initialDescribeOutput.indexOf("observers info:"))
    assertEquals(3, votersInfo.split("\n").length)

    val observersInfo = initialDescribeOutput.substring(initialDescribeOutput.indexOf("observers info:") + 16)
    assertTrue(observersInfo.isEmpty, "this will be fixed by KAFKA-13986")
  }

  @ClusterTest(clusterType = Type.ZK, brokers = 3, controllers = 1)
  def testDescribeQuorumInZkMode(): Unit = {
    assertTrue(
      assertThrows(
        classOf[ExecutionException],
        () => MetadataQuorumCommand.mainNoExit(Array("--bootstrap-server", cluster.bootstrapServers(), "describe"))
      ).getCause.isInstanceOf[UnsupportedVersionException]
    )
  }
}

class MetadataQuorumCommandErrorTest {

  @Test
  def testPropertiesFileDoesNotExists(): Unit = {
    assertEquals(
      1,
      MetadataQuorumCommand.mainNoExit(Array("--bootstrap-server", "localhost:9092", "--command-config", "admin.properties", "describe"))
    )
    assertEquals(
      "Properties file admin.properties does not exists!",
      TestUtils.grabConsoleError(MetadataQuorumCommand.mainNoExit(Array("--bootstrap-server", "localhost:9092", "--command-config", "admin.properties", "describe"))).trim
    )
  }
}