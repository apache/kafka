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

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import javax.security.auth.login.LoginException
import joptsimple.OptionParser
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{ToolsUtils, Logging, ZKGroupTopicDirs, ZkUtils, CommandLineUtils, ZkPath}
import org.apache.kafka.common.security.JaasUtils

import org.apache.zookeeper.AsyncCallback.{ChildrenCallback, StatCallback}
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.ZooKeeper

import scala.collection.JavaConverters._
import scala.collection._
import scala.concurrent._

object ZkSecurityMigrator extends Logging {
  val workQueue = new LinkedBlockingQueue[Runnable]
  val threadExecutionContext = 
    ExecutionContext.fromExecutor(new ThreadPoolExecutor(1,
                                                         Runtime.getRuntime().availableProcessors(),
                                                         5000,
                                                         TimeUnit.MILLISECONDS,
                                                         workQueue))

  def setAclsRecursively(zkUtils: ZkUtils, path: String) {
    zkUtils.zkConnection.getZookeeper.getChildren(path, false, GetChildrenCallback, zkUtils)
  }

  object GetChildrenCallback extends ChildrenCallback {
      def processResult(rc: Int,
                        path: String,
                        ctx: Object,
                        children: java.util.List[String]) {
        val list = children.asScala.toList
        val zkUtils: ZkUtils = ctx.asInstanceOf[ZkUtils]
        val zkHandle = zkUtils.zkConnection.getZookeeper
        
        Code.get(rc) match {
          case Code.OK =>
            // Set ACL for each child
            Future {
              for(child <- list) {
                zkHandle.setACL(child, ZkUtils.DefaultAcls(true), -1, SetACLCallback, ctx)
                zkHandle.getChildren(child, false, GetChildrenCallback, ctx)
              }
            }(threadExecutionContext)
          case Code.CONNECTIONLOSS =>
            setAclsRecursively(zkUtils, path)
          case Code.NONODE =>
            warn("Node is gone, it could be have been legitimately deleted: %s".format(path))
          case Code.SESSIONEXPIRED =>
            // Starting a new session isn't really a problem, but it'd complicate
            // the logic of the tool, so we quit and let the user re-run it.
            error("ZooKeeper session expired while changing ACLs")
            System.exit(1)
          case _ =>
            error("Unexpected return code: %d".format(rc))
            System.exit(1)
        }
      }
  }
  
  object SetACLCallback extends StatCallback {
    def processResult(rc: Int,
                      path: String,
                      ctx: Object,
                      stat: Stat) {
      val zkUtils: ZkUtils = ctx.asInstanceOf[ZkUtils]
      val zkHandle = zkUtils.zkConnection.getZookeeper
        
      Code.get(rc) match {
          case Code.OK =>
            info("Successfully set ACLs for %s".format(path))
          case Code.CONNECTIONLOSS =>
            Future {
              zkHandle.setACL(path, ZkUtils.DefaultAcls(true), -1, SetACLCallback, ctx)
            }(threadExecutionContext)
          case Code.NONODE =>
            warn("Node is gone, it could be have been legitimately deleted: %s".format(path))
          case Code.SESSIONEXPIRED =>
            // Starting a new session isn't really a problem, but it'd complicate
            // the logic of the tool, so we quit and let the user re-run it.
            error("ZooKeeper session expired while changing ACLs")
            System.exit(1)
          case _ =>
            error("Unexpected return code: %d".format(rc))
            System.exit(1)   
      }
    }
  }
  
  def main(args: Array[String]) {
    val parser = new OptionParser()

    val jaasFileOpt = parser.accepts("jaas.file", "JAAS Config file.").
      withRequiredArg().ofType(classOf[String])
    val zkUrlOpt = parser.accepts("connect", "Sets the ZooKeeper connect string (ensemble). This parameter " +
                                  "takes a comma-separated list of host:port pairs.").
      withRequiredArg().defaultsTo("localhost:2181").ofType(classOf[String])
    val zkSessionTimeoutOpt = parser.accepts("session.timeout", "Sets the ZooKeeper session timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[Integer])
    val zkConnectionTimeoutOpt = parser.accepts("connection.timeout", "Sets the ZooKeeper connection timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[Integer])
    val helpOpt = parser.accepts("help", "Print usage information.")

    val options = parser.parse(args : _*)
    if(options.has(helpOpt))
      CommandLineUtils.printUsageAndDie(parser, "ZooKeeper Migration Tool Help")

    if(!options.has(jaasFileOpt) ||
      !JaasUtils.isZkSecurityEnabled(options.valueOf(jaasFileOpt))) {
      error("No JAAS configuration file has been found. Please make sure that "
            + "you have set the option --jaas.file correctly and that the file"
            + " is valid")
      System.exit(1) 
    }

    val jaasFile = options.valueOf(jaasFileOpt)
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile)
    val zkUrl = options.valueOf(zkUrlOpt)
    val zkSessionTimeout = options.valueOf(zkSessionTimeoutOpt)
    val zkConnectionTimeout = options.valueOf(zkConnectionTimeoutOpt)

    val zkUtils = ZkUtils.apply(zkUrl, zkSessionTimeout, zkConnectionTimeout, true)
    for (path <- zkUtils.persistentZkPaths) {
      zkUtils.makeSurePersistentPathExists(path)
      setAclsRecursively(zkUtils, path)
    } 
  }
}