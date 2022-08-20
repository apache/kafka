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
      new ClusterTest(clusterType = Type.CO_KRAFT, brokers = 3, controllers = 2),
      new ClusterTest(clusterType = Type.KRAFT, brokers = 3, controllers = 2),
      new ClusterTest(clusterType = Type.CO_KRAFT, brokers = 2, controllers = 3),
      new ClusterTest(clusterType = Type.KRAFT, brokers = 2, controllers = 3)
    ))
  def testDescribeQuorumReplicationSuccessful(): Unit = {
    cluster.waitForReadyBrokers()
    val describeOutput = TestUtils.grabConsoleOutput(
      MetadataQuorumCommand.mainNoExit(
        Array("--bootstrap-server", cluster.bootstrapServers(), "describe", "--replication"))
    )

    val leaderPattern = """\d+\s+\d+\s+\d+\s+\d+\s+[-]?\d+\s+Leader\s+""".r
    val followerPattern = """\d+\s+\d+\s+\d+\s+\d+\s+[-]?\d+\s+Follower\s+""".r
    val observerPattern = """\d+\s+\d+\s+\d+\s+\d+\s+[-]?\d+\s+Observer\s+""".r
    val outputs = describeOutput.split("\n").tail
    if (cluster.config().clusterType() == Type.CO_KRAFT) {
      assertEquals(Math.max(cluster.config().numControllers(), cluster.config().numBrokers()), outputs.length)
    } else {
      assertEquals(cluster.config().numBrokers() + cluster.config().numControllers(), outputs.length)
    }
    // `matches` is not supported in scala 2.12, use `findFirstIn` instead.
    assertTrue(leaderPattern.findFirstIn(outputs.head).nonEmpty)
    assertEquals(1, outputs.count(leaderPattern.findFirstIn(_).nonEmpty))
    assertEquals(cluster.config().numControllers() - 1, outputs.count(followerPattern.findFirstIn(_).nonEmpty))

    if (cluster.config().clusterType() == Type.CO_KRAFT) {
      assertEquals(Math.max(0, cluster.config().numBrokers() - cluster.config().numControllers()), outputs.count(observerPattern.findFirstIn(_).nonEmpty))
    } else {
      assertEquals(cluster.config().numBrokers(), outputs.count(observerPattern.findFirstIn(_).nonEmpty))
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
      new ClusterTest(clusterType = Type.CO_KRAFT, brokers = 3, controllers = 2),
      new ClusterTest(clusterType = Type.KRAFT, brokers = 3, controllers = 2),
      new ClusterTest(clusterType = Type.CO_KRAFT, brokers = 2, controllers = 3),
      new ClusterTest(clusterType = Type.KRAFT, brokers = 2, controllers = 3)
    ))
  def testDescribeQuorumStatusSuccessful(): Unit = {
    cluster.waitForReadyBrokers()
    val describeOutput = TestUtils.grabConsoleOutput(
      MetadataQuorumCommand.mainNoExit(Array("--bootstrap-server", cluster.bootstrapServers(), "describe", "--status"))
    )
    val outputs = describeOutput.split("\n")

    assertTrue("""ClusterId:\s+\S{22}""".r.findFirstIn(outputs(0)).nonEmpty)
    assertTrue("""LeaderId:\s+\d+""".r.findFirstIn(outputs(1)).nonEmpty)
    assertTrue("""LeaderEpoch:\s+\d+""".r.findFirstIn(outputs(2)).nonEmpty)
    // HighWatermark may be -1
    assertTrue("""HighWatermark:\s+[-]?\d+""".r.findFirstIn(outputs(3)).nonEmpty)
    assertTrue("""MaxFollowerLag:\s+\d+""".r.findFirstIn(outputs(4)).nonEmpty)
    assertTrue("""MaxFollowerLagTimeMs:\s+[-]?\d+""".r.findFirstIn(outputs(5)).nonEmpty)
    assertTrue("""CurrentVoters:\s+\[\d+(,\d+)*\]""".r.findFirstIn(outputs(6)).nonEmpty)

    // There are no observers if we have fewer brokers than controllers
    if (cluster.config().clusterType() == Type.CO_KRAFT
        && cluster.config().numBrokers() <= cluster.config().numControllers()) {
      assertTrue("""CurrentObservers:\s+\[\]""".r.findFirstIn(outputs(7)).nonEmpty)
    } else {
      assertTrue("""CurrentObservers:\s+\[\d+(,\d+)*\]""".r.findFirstIn(outputs(7)).nonEmpty)
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
    assertEquals("0", replicationOutput.split("\n")(1).split("\\s+")(2))
  }

  @ClusterTest(clusterType = Type.ZK, brokers = 3)
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
