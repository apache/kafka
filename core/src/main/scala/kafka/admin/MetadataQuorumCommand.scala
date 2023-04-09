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

import kafka.tools.TerseFailure
import kafka.utils.Exit
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.{fileType, storeTrue}
import net.sourceforge.argparse4j.inf.Subparsers
import org.apache.kafka.clients._
import org.apache.kafka.clients.admin.{Admin, QuorumInfo}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.util.ToolsUtils.prettyPrintTable

import java.io.File
import java.util.Properties
import scala.jdk.CollectionConverters._

/**
 * A tool for describing quorum status
 */
object MetadataQuorumCommand {

  def main(args: Array[String]): Unit = {
    val res = mainNoExit(args)
    Exit.exit(res)
  }

  def mainNoExit(args: Array[String]): Int = {
    val parser = ArgumentParsers
      .newArgumentParser("kafka-metadata-quorum")
      .defaultHelp(true)
      .description("This tool describes kraft metadata quorum status.")
    parser
      .addArgument("--bootstrap-server")
      .help("A comma-separated list of host:port pairs to use for establishing the connection to the Kafka cluster.")
      .required(true)

    parser
      .addArgument("--command-config")
      .`type`(fileType())
      .help("Property file containing configs to be passed to Admin Client.")
    val subparsers = parser.addSubparsers().dest("command")
    addDescribeParser(subparsers)

    var admin: Admin = null
    try {
      val namespace = parser.parseArgsOrFail(args)
      val command = namespace.getString("command")

      val commandConfig = namespace.get[File]("command_config")
      val props = if (commandConfig != null) {
        if (!commandConfig.exists()) {
          throw new TerseFailure(s"Properties file ${commandConfig.getPath} does not exists!")
        }
        Utils.loadProps(commandConfig.getPath)
      } else {
        new Properties()
      }
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, namespace.getString("bootstrap_server"))
      admin = Admin.create(props)

      if (command == "describe") {
        if (namespace.getBoolean("status") && namespace.getBoolean("replication")) {
          throw new TerseFailure(s"Only one of --status or --replication should be specified with describe sub-command")
        } else if (namespace.getBoolean("replication")) {
          handleDescribeReplication(admin)
        } else if (namespace.getBoolean("status")) {
          handleDescribeStatus(admin)
        } else {
          throw new TerseFailure(s"One of --status or --replication must be specified with describe sub-command")
        }
      } else {
        throw new IllegalStateException(s"Unknown command: $command, only 'describe' is supported")
      }
      0
    } catch {
      case e: TerseFailure =>
        Console.err.println(e.getMessage)
        1
    } finally {
      if (admin != null) {
        admin.close()
      }
    }
  }

  def addDescribeParser(subparsers: Subparsers): Unit = {
    val describeParser = subparsers
      .addParser("describe")
      .help("Describe the metadata quorum info")

    val statusArgs = describeParser.addArgumentGroup("Status")
    statusArgs
      .addArgument("--status")
      .help(
        "A short summary of the quorum status and the other provides detailed information about the status of replication.")
      .action(storeTrue())
    val replicationArgs = describeParser.addArgumentGroup("Replication")
    replicationArgs
      .addArgument("--replication")
      .help("Detailed information about the status of replication")
      .action(storeTrue())
  }

  private def handleDescribeReplication(admin: Admin): Unit = {
    val quorumInfo = admin.describeMetadataQuorum.quorumInfo.get
    val leaderId = quorumInfo.leaderId
    val leader = quorumInfo.voters.asScala.filter(_.replicaId == leaderId).head

    def convertQuorumInfo(infos: Seq[QuorumInfo.ReplicaState], status: String): Seq[Array[String]] =
      infos.map { info =>
        Array(info.replicaId,
              info.logEndOffset,
              leader.logEndOffset - info.logEndOffset,
              info.lastFetchTimestamp.orElse(-1),
              info.lastCaughtUpTimestamp.orElse(-1),
              status
        ).map(_.toString)
      }
    prettyPrintTable(
      Array("NodeId", "LogEndOffset", "Lag", "LastFetchTimestamp", "LastCaughtUpTimestamp", "Status"),
      (convertQuorumInfo(Seq(leader), "Leader")
        ++ convertQuorumInfo(quorumInfo.voters.asScala.filter(_.replicaId != leaderId).toSeq, "Follower")
        ++ convertQuorumInfo(quorumInfo.observers.asScala.toSeq, "Observer")).asJava,
      scala.Console.out
    )
  }

  private def handleDescribeStatus(admin: Admin): Unit = {
    val clusterId = admin.describeCluster.clusterId.get
    val quorumInfo = admin.describeMetadataQuorum.quorumInfo.get
    val leaderId = quorumInfo.leaderId
    val leader = quorumInfo.voters.asScala.filter(_.replicaId == leaderId).head
    val maxLagFollower = quorumInfo.voters.asScala
      .minBy(_.logEndOffset)
    val maxFollowerLag = leader.logEndOffset - maxLagFollower.logEndOffset
    val maxFollowerLagTimeMs =
      if (leader == maxLagFollower) {
        0
      } else if (leader.lastCaughtUpTimestamp.isPresent && maxLagFollower.lastCaughtUpTimestamp.isPresent) {
        leader.lastCaughtUpTimestamp.getAsLong - maxLagFollower.lastCaughtUpTimestamp.getAsLong
      } else {
        -1
      }
    println(
      s"""|ClusterId:              $clusterId
          |LeaderId:               ${quorumInfo.leaderId}
          |LeaderEpoch:            ${quorumInfo.leaderEpoch}
          |HighWatermark:          ${quorumInfo.highWatermark}
          |MaxFollowerLag:         $maxFollowerLag
          |MaxFollowerLagTimeMs:   $maxFollowerLagTimeMs
          |CurrentVoters:          ${quorumInfo.voters.asScala.map(_.replicaId).mkString("[", ",", "]")}
          |CurrentObservers:       ${quorumInfo.observers.asScala.map(_.replicaId).mkString("[", ",", "]")}
          |""".stripMargin
    )
  }
}
