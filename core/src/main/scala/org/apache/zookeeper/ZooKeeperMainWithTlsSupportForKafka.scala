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

package org.apache.zookeeper

import kafka.admin.ZkSecurityMigrator
import org.apache.zookeeper.admin.ZooKeeperAdmin
import org.apache.zookeeper.cli.CommandNotFoundException
import org.apache.zookeeper.cli.MalformedCommandException
import org.apache.zookeeper.client.ZKClientConfig

import scala.jdk.CollectionConverters._

object ZooKeeperMainWithTlsSupportForKafka {
  private val zkTlsConfigFileOption = "-zk-tls-config-file"
  def main(args: Array[String]): Unit = {
    val zkTlsConfigFileIndex = args.indexOf(zkTlsConfigFileOption)
    val zooKeeperMain: ZooKeeperMain =
      if (zkTlsConfigFileIndex < 0)
        // no TLS config, so just pass args directly
        new ZooKeeperMainWithTlsSupportForKafka(args, None)
      else if (zkTlsConfigFileIndex == args.length - 1)
        throw new IllegalArgumentException(s"Error: no filename provided with option $zkTlsConfigFileOption")
      else
        // found TLS config, so instantiate it and pass args without the two TLS config-related arguments
        new ZooKeeperMainWithTlsSupportForKafka(
          args.slice(0, zkTlsConfigFileIndex) ++ args.slice(zkTlsConfigFileIndex + 2, args.length),
          Some(ZkSecurityMigrator.createZkClientConfigFromFile(args(zkTlsConfigFileIndex + 1))))
    // The run method of ZooKeeperMain is package-private,
    // therefore this code unfortunately must reside in the same org.apache.zookeeper package.
    zooKeeperMain.run()
  }
}

class ZooKeeperMainWithTlsSupportForKafka(args: Array[String], val zkClientConfig: Option[ZKClientConfig])
  extends ZooKeeperMain(args) with Watcher {

  override def processZKCmd (co: ZooKeeperMain.MyCommandOptions): Boolean = {
    // Unfortunately the usage() method is static, so it can't be overridden.
    // This method is where usage() gets called.  We don't cover all possible calls
    // to usage() -- we would have to implement the entire method to do that -- but
    // the short implementation below covers most cases.
    val args = co.getArgArray
    val cmd = co.getCommand
    if (args.length < 1) {
      kafkaTlsUsage()
      throw new MalformedCommandException("No command entered")
    }

    if (!ZooKeeperMain.commandMap.containsKey(cmd)) {
      kafkaTlsUsage()
      throw new CommandNotFoundException(s"Command not found $cmd")
    }
    super.processZKCmd(co)
  }

  private def kafkaTlsUsage(): Unit = {
    System.err.println("ZooKeeper -server host:port [-zk-tls-config-file <file>] cmd args")
    ZooKeeperMain.commandMap.keySet.asScala.toList.sorted.foreach(cmd =>
      System.err.println(s"\t$cmd ${ZooKeeperMain.commandMap.get(cmd)}"))
  }

  override def connectToZK(newHost: String): Unit = {
    // ZooKeeperAdmin has no constructor that supports passing in both readOnly and ZkClientConfig,
    // and readOnly ends up being set to false when passing in a ZkClientConfig instance;
    // therefore it is currently not possible for us to construct a ZooKeeperAdmin instance with
    // both an explicit ZkClientConfig instance and a readOnly value of true.
    val readOnlyRequested = cl.getOption("readonly") != null
    if (readOnlyRequested && zkClientConfig.isDefined)
      throw new IllegalArgumentException(
        s"read-only mode (-r) is not supported with an explicit TLS config (${ZooKeeperMainWithTlsSupportForKafka.zkTlsConfigFileOption})")
    if (zk != null && zk.getState.isAlive) zk.close()
    host = newHost
    zk = if (zkClientConfig.isDefined)
      new ZooKeeperAdmin(host, cl.getOption("timeout").toInt, this, zkClientConfig.get)
    else
      new ZooKeeperAdmin(host, cl.getOption("timeout").toInt, this, readOnlyRequested)
  }

  override def process(event: WatchedEvent): Unit = {
    if (getPrintWatches) {
      ZooKeeperMain.printMessage("WATCHER::")
      ZooKeeperMain.printMessage(event.toString)
    }
  }
}
