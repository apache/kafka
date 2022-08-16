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

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT)
@Tag("integration")
class MetadataQuorumCommandTest(cluster: ClusterInstance) {

  /**
   * 1. The same number of broker controllers
   * 2. More brokers than controllers
   * 3. Fewer brokers than controllers
   */
  @ClusterTests(
    Array(
      new ClusterTest(clusterType = Type.CO_KRAFT, brokers = 3, controllers = 3),
      new ClusterTest(clusterType = Type.KRAFT, brokers = 3, controllers = 3),
      new ClusterTest(clusterType = Type.CO_KRAFT, brokers = 3, controllers = 4),
      new ClusterTest(clusterType = Type.KRAFT, brokers = 3, controllers = 4),
      new ClusterTest(clusterType = Type.CO_KRAFT, brokers = 4, controllers = 3),
      new ClusterTest(clusterType = Type.KRAFT, brokers = 4, controllers = 3)
    ))
  def testDescribeQuorumReplicationSuccessful(): Unit = {
    val describeOutput = TestUtils.grabConsoleOutput(
      MetadataQuorumCommand.mainNoExit(
        Array("--bootstrap-server", cluster.bootstrapServers(), "describe", "--replication"))
    )

    val leaderPattern = """\d+\s+\d+\s+\d+\s+[-]?\d+\s+[-]?\d+\s+Leader\s+""".r
    val followerPattern = """\d+\s+\d+\s+\d+\s+[-]?\d+\s+[-]?\d+\s+Follower\s+""".r
    val observerPattern = """\d+\s+\d+\s+\d+\s+[-]?\d+\s+[-]?\d+\s+Observer\s+""".r
    val outputs = describeOutput.split("\n").tail
    if (cluster.config().clusterType() == Type.CO_KRAFT) {
      assertEquals(Math.max(cluster.config().numControllers(), cluster.config().numBrokers()), outputs.length)
    } else {
      assertEquals(cluster.config().numBrokers() + cluster.config().numControllers(), outputs.length)
    }
    assertTrue(leaderPattern.matches(outputs.head), "[" + outputs.head + "]")
    assertEquals(1, outputs.count(leaderPattern.matches(_)))
    assertEquals(cluster.config().numControllers() - 1, outputs.count(followerPattern.matches(_)))

    if (cluster.config().clusterType() == Type.CO_KRAFT) {
      assertEquals(Math.max(0, cluster.config().numBrokers() - cluster.config().numControllers()), outputs.count(observerPattern.matches(_)))
    } else {
      assertEquals(cluster.config().numBrokers(), outputs.count(observerPattern.matches(_)))
    }
  }

  /**
   * 1. The same number of broker controllers
   * 2. More brokers than controllers
   * 3. Fewer brokers than controllers
   */
  @ClusterTests(
    Array(
      new ClusterTest(clusterType = Type.CO_KRAFT, brokers = 3, controllers = 3),
      new ClusterTest(clusterType = Type.KRAFT, brokers = 3, controllers = 3),
      new ClusterTest(clusterType = Type.CO_KRAFT, brokers = 3, controllers = 4),
      new ClusterTest(clusterType = Type.KRAFT, brokers = 3, controllers = 4),
      new ClusterTest(clusterType = Type.CO_KRAFT, brokers = 4, controllers = 3),
      new ClusterTest(clusterType = Type.KRAFT, brokers = 4, controllers = 3)
    ))
  def testDescribeQuorumStatusSuccessful(): Unit = {
    val describeOutput = TestUtils.grabConsoleOutput(
      MetadataQuorumCommand.mainNoExit(Array("--bootstrap-server", cluster.bootstrapServers(), "describe", "--status"))
    )
    val outputs = describeOutput.split("\n")

    assertTrue("""ClusterId:\s+\S{22}""".r.matches(outputs(0)))
    assertTrue("""LeaderId:\s+\d+""".r.matches(outputs(1)))
    assertTrue("""LeaderEpoch:\s+\d+""".r.matches(outputs(2)))
    assertTrue("""HighWatermark:\s+\d+""".r.matches(outputs(3)))
    assertTrue("""MaxFollowerLag:\s+\d+""".r.matches(outputs(4)))
    assertTrue("""MaxFollowerLagTimeMs:\s+[-]?\d+""".r.matches(outputs(5)), "[" + outputs(5) + "]")
    assertTrue("""CurrentVoters:\s+\[\d+(,\d+)*\]""".r.matches(outputs(6)))

    // There are no observers if we have fewer brokers than controllers
    if (cluster.config().clusterType() == Type.CO_KRAFT
        && cluster.config().numBrokers() <= cluster.config().numControllers()) {
      assertTrue("""CurrentObservers:\s+\[\]""".r.matches(outputs(7)))
    } else {
      assertTrue("""CurrentObservers:\s+\[\d+(,\d+)*\]""".r.matches(outputs(7)))
    }
  }

  @ClusterTests(
    Array(new ClusterTest(clusterType = Type.CO_KRAFT, brokers = 1, controllers = 1),
          new ClusterTest(clusterType = Type.KRAFT, brokers = 1, controllers = 1)))
  def testOnlyOneBrokerAndOneController(): Unit = {
    val statusOutput = TestUtils.grabConsoleOutput(
      MetadataQuorumCommand.mainNoExit(Array("--bootstrap-server", cluster.bootstrapServers(), "describe", "--status"))
    )
    assertEquals("MaxFollowerLag:         0", statusOutput.split("\n")(4))
    assertEquals("MaxFollowerLagTimeMs:   0", statusOutput.split("\n")(5))

    val replicationOutput = TestUtils.grabConsoleOutput(
      MetadataQuorumCommand.mainNoExit(Array("--bootstrap-server", cluster.bootstrapServers(), "describe", "--replication"))
    )
    assertEquals("0", replicationOutput.split("\n").last.split("\\s+")(2))
  }

  @ClusterTest(clusterType = Type.ZK, brokers = 3, controllers = 1)
  def testDescribeQuorumInZkMode(): Unit = {
    assertTrue(
      assertThrows(
        classOf[ExecutionException],
        () =>
          MetadataQuorumCommand.mainNoExit(
            Array("--bootstrap-server", cluster.bootstrapServers(), "describe", "--status"))
      ).getCause.isInstanceOf[UnsupportedVersionException]
    )
    assertTrue(
      assertThrows(
        classOf[ExecutionException],
        () =>
          MetadataQuorumCommand.mainNoExit(
            Array("--bootstrap-server", cluster.bootstrapServers(), "describe", "--replication"))
      ).getCause.isInstanceOf[UnsupportedVersionException]
    )
  }
}

class MetadataQuorumCommandErrorTest {

  @Test
  def testPropertiesFileDoesNotExists(): Unit = {
    assertEquals(1,
                 MetadataQuorumCommand.mainNoExit(
                   Array("--bootstrap-server", "localhost:9092", "--command-config", "admin.properties", "describe")))
    assertEquals(
      "Properties file admin.properties does not exists!",
      TestUtils
        .grabConsoleError(
          MetadataQuorumCommand.mainNoExit(
            Array("--bootstrap-server", "localhost:9092", "--command-config", "admin.properties", "describe")))
        .trim
    )
  }

  @Test
  def testDescribeOptions(): Unit = {
    assertEquals(1, MetadataQuorumCommand.mainNoExit(Array("--bootstrap-server", "localhost:9092", "describe")))
    assertEquals(
      "One of --status or --replication must be specified with describe sub-command",
      TestUtils
        .grabConsoleError(MetadataQuorumCommand.mainNoExit(Array("--bootstrap-server", "localhost:9092", "describe")))
        .trim
    )

    assertEquals(1,
                 MetadataQuorumCommand.mainNoExit(
                   Array("--bootstrap-server", "localhost:9092", "describe", "--status", "--replication")))
    assertEquals(
      "Only one of --status or --replication should be specified with describe sub-command",
      TestUtils
        .grabConsoleError(
          MetadataQuorumCommand.mainNoExit(
            Array("--bootstrap-server", "localhost:9092", "describe", "--status", "--replication")))
        .trim
    )
  }
}
