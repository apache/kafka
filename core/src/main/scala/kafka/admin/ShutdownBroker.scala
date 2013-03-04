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


import joptsimple.OptionParser
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import javax.management.remote.{JMXServiceURL, JMXConnectorFactory}
import javax.management.ObjectName
import kafka.controller.KafkaController
import scala.Some
import kafka.common.BrokerNotAvailableException


object ShutdownBroker extends Logging {

  private case class ShutdownParams(zkConnect: String, brokerId: java.lang.Integer)

  private def invokeShutdown(params: ShutdownParams): Boolean = {
    var zkClient: ZkClient = null
    try {
      zkClient = new ZkClient(params.zkConnect, 30000, 30000, ZKStringSerializer)
      val controllerBrokerId = ZkUtils.getController(zkClient)
      ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + controllerBrokerId)._1 match {
        case Some(controllerInfo) =>
          var controllerHost: String = null
          var controllerJmxPort: Int = -1
          try {
            Json.parseFull(controllerInfo) match {
              case Some(m) =>
                val brokerInfo = m.asInstanceOf[Map[String, Any]]
                controllerHost = brokerInfo.get("host").get.toString
                controllerJmxPort = brokerInfo.get("jmx_port").get.asInstanceOf[Int]
              case None =>
                throw new BrokerNotAvailableException("Broker id %d does not exist".format(controllerBrokerId))
            }
          }
          val jmxUrl = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi".format(controllerHost, controllerJmxPort))
          info("Connecting to jmx url " + jmxUrl)
          val jmxc = JMXConnectorFactory.connect(jmxUrl, null)
          val mbsc = jmxc.getMBeanServerConnection
          val leaderPartitionsRemaining = mbsc.invoke(new ObjectName(KafkaController.MBeanName),
                                                      "shutdownBroker",
                                                      Array(params.brokerId),
                                                      Array(classOf[Int].getName)).asInstanceOf[Int]
          val shutdownComplete = (leaderPartitionsRemaining == 0)
          info("Shutdown status: " +
            (if (shutdownComplete) "complete" else "incomplete (broker still leads %d partitions)".format(leaderPartitionsRemaining)))
          shutdownComplete
        case None =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(controllerBrokerId))
      }
    } catch {
      case t: Throwable =>
        error("Operation failed due to controller failure", t)
        false
    } finally {
      if (zkClient != null)
        zkClient.close()
    }
  }

  def main(args: Array[String]) {
    val parser = new OptionParser
    val brokerOpt = parser.accepts("broker", "REQUIRED: The broker to shutdown.")
            .withRequiredArg
            .describedAs("Broker Id")
            .ofType(classOf[java.lang.Integer])
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
            "Multiple URLS can be given to allow fail-over.")
            .withRequiredArg
            .describedAs("urls")
            .ofType(classOf[String])
    val numRetriesOpt = parser.accepts("num.retries", "Number of attempts to retry if shutdown does not complete.")
            .withRequiredArg
            .describedAs("number of retries")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(0)
    val retryIntervalOpt = parser.accepts("retry.interval.ms", "Retry interval if retries requested.")
            .withRequiredArg
            .describedAs("retry interval in ms (> 1000)")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(1000)

    val options = parser.parse(args : _*)
    CommandLineUtils.checkRequiredArgs(parser, options, brokerOpt, zkConnectOpt)

    val retryIntervalMs = options.valueOf(retryIntervalOpt).intValue.max(1000)
    val numRetries = options.valueOf(numRetriesOpt).intValue

    val shutdownParams = ShutdownParams(options.valueOf(zkConnectOpt), options.valueOf(brokerOpt))

    if (!invokeShutdown(shutdownParams)) {
      (1 to numRetries).takeWhile(attempt => {
        info("Retry " + attempt)
        try {
          Thread.sleep(retryIntervalMs)
        }
        catch {
          case ie: InterruptedException => // ignore
        }
        !invokeShutdown(shutdownParams)
      })
    }
  }

}

