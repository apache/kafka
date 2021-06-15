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

package kafka.tools

import java.io.PrintStream
import java.util.Properties
import java.util.concurrent.ExecutionException

import kafka.utils.{Exit, Logging}
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.store
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.utils.Utils

object ClusterTool extends Logging {
  def main(args: Array[String]): Unit = {
    try {
      val parser = ArgumentParsers.
        newArgumentParser("kafka-cluster").
        defaultHelp(true).
        description("The Kafka cluster tool.")
      val subparsers = parser.addSubparsers().dest("command")

      val clusterIdParser = subparsers.addParser("cluster-id").
        help("Get information about the ID of a cluster.")
      val unregisterParser = subparsers.addParser("unregister").
        help("Unregister a broker.")
      List(clusterIdParser, unregisterParser).foreach(parser => {
        parser.addArgument("--bootstrap-server", "-b").
          action(store()).
          help("A list of host/port pairs to use for establishing the connection to the kafka cluster.")
        parser.addArgument("--config", "-c").
          action(store()).
          help("A property file containing configs to passed to AdminClient.")
      })
      unregisterParser.addArgument("--id", "-i").
        `type`(classOf[Integer]).
        action(store()).
        help("The ID of the broker to unregister.")

      val namespace = parser.parseArgsOrFail(args)
      val command = namespace.getString("command")
      val configPath = namespace.getString("config")
      val properties = if (configPath == null) {
        new Properties()
      } else {
        Utils.loadProps(configPath)
      }
      Option(namespace.getString("bootstrap_server")).
        foreach(b => properties.setProperty("bootstrap.servers", b))
      if (properties.getProperty("bootstrap.servers") == null) {
        throw new TerseFailure("Please specify --bootstrap-server.")
      }

      command match {
        case "cluster-id" =>
          val adminClient = Admin.create(properties)
          try {
            clusterIdCommand(System.out, adminClient)
          } finally {
            adminClient.close()
          }
          Exit.exit(0)
        case "unregister" =>
          val adminClient = Admin.create(properties)
          try {
            unregisterCommand(System.out, adminClient, namespace.getInt("id"))
          } finally {
            adminClient.close()
          }
          Exit.exit(0)
        case _ =>
          throw new RuntimeException(s"Unknown command $command")
      }
    } catch {
      case e: TerseFailure =>
        System.err.println(e.getMessage)
        System.exit(1)
    }
  }

  def clusterIdCommand(stream: PrintStream,
                       adminClient: Admin): Unit = {
    val clusterId = Option(adminClient.describeCluster().clusterId().get())
    clusterId match {
      case None => stream.println(s"No cluster ID found. The Kafka version is probably too old.")
      case Some(id) => stream.println(s"Cluster ID: ${id}")
    }
  }

  def unregisterCommand(stream: PrintStream,
                          adminClient: Admin,
                          id: Int): Unit = {
    try {
      Option(adminClient.unregisterBroker(id).all().get())
      stream.println(s"Broker ${id} is no longer registered.")
    } catch {
      case e: ExecutionException => {
        val cause = e.getCause()
        if (cause.isInstanceOf[UnsupportedVersionException]) {
          stream.println(s"The target cluster does not support the broker unregistration API.")
        } else {
          throw e
        }
      }
    }
  }
}
