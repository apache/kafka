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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import javax.security.auth.login.LoginException
import joptsimple.OptionParser
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkException
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
  val counter = new AtomicInteger(0)
  var exception: ZkException = null
  val workQueue = new LinkedBlockingQueue[Runnable]
  val threadExecutionContext = 
    ExecutionContext.fromExecutor(new ThreadPoolExecutor(1,
                                                         Runtime.getRuntime().availableProcessors(),
                                                         5000,
                                                         TimeUnit.MILLISECONDS,
                                                         workQueue))

  def setAclsRecursively(zkUtils: ZkUtils, path: String) {
    counter.incrementAndGet()
    zkUtils.zkConnection.getZookeeper.getChildren(path, false, GetChildrenCallback, zkUtils)
  }

  object GetChildrenCallback extends ChildrenCallback {
      def processResult(rc: Int,
                        path: String,
                        ctx: Object,
                        children: java.util.List[String]) {
        val list = if(children == null)
          null
        else
          children.asScala.toList
        val zkUtils: ZkUtils = ctx.asInstanceOf[ZkUtils]
        val zkHandle = zkUtils.zkConnection.getZookeeper
        
        Code.get(rc) match {
          case Code.OK =>
            // Set ACL for each child
            if(list != null) 
              Future {
                val childPathBuilder = new StringBuilder
                for(child <- list) {
                  childPathBuilder.clear
                  childPathBuilder.append(path)
                  childPathBuilder.append("/")
                  childPathBuilder.append(child)

                  val childPath = childPathBuilder.toString
                  counter.incrementAndGet()
                  zkHandle.setACL(childPath, ZkUtils.DefaultAcls(true), -1, SetACLCallback, ctx)
                  zkHandle.getChildren(childPath, false, GetChildrenCallback, ctx)
                }
                counter.decrementAndGet()
              }(threadExecutionContext)
          case Code.CONNECTIONLOSS =>
            setAclsRecursively(zkUtils, path)
          case Code.NONODE =>
            warn("Node is gone, it could be have been legitimately deleted: %s".format(path))
          case Code.SESSIONEXPIRED =>
            // Starting a new session isn't really a problem, but it'd complicate
            // the logic of the tool, so we quit and let the user re-run it.
            error("ZooKeeper session expired while changing ACLs")
            exception = throw ZkException.create(KeeperException.create(rc))
          case _ =>
            error("Unexpected return code: %d".format(rc))
            exception = throw ZkException.create(KeeperException.create(rc))
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
            counter.decrementAndGet()
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
            exception = throw ZkException.create(KeeperException.create(rc))
          case _ =>
            error("Unexpected return code: %d".format(rc))
            exception = throw ZkException.create(KeeperException.create(rc))   
      }
    }
  }
  
  def run(args: Array[String]) {
    var jaasFile = System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    val parser = new OptionParser()

    val jaasFileOpt = parser.accepts("jaas.file", "JAAS Config file.").withOptionalArg().ofType(classOf[String])
    val zkUrlOpt = parser.accepts("connect", "Sets the ZooKeeper connect string (ensemble). This parameter " +
                                  "takes a comma-separated list of host:port pairs.").
      withRequiredArg().defaultsTo("localhost:2181").ofType(classOf[String])
    val zkSessionTimeoutOpt = parser.accepts("session.timeout", "Sets the ZooKeeper session timeout.").
      withRequiredArg().ofType(classOf[java.lang.Integer])
    val zkConnectionTimeoutOpt = parser.accepts("connection.timeout", "Sets the ZooKeeper connection timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[java.lang.Integer])
    val helpOpt = parser.accepts("help", "Print usage information.")

    val options = parser.parse(args : _*)
    if(options.has(helpOpt))
      CommandLineUtils.printUsageAndDie(parser, "ZooKeeper Migration Tool Help")

    if ((jaasFile == null) && !options.has(jaasFileOpt)) {
      error("No JAAS configuration file has been specified. Please make sure that you have set either "
            + "the system property %s or the option %s".format(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, "--jaas.file"))
      throw new IllegalArgumentException("Incorrect configuration")
    }
    
    if (jaasFile == null) {
      jaasFile = options.valueOf(jaasFileOpt)
      System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile)
    }
    
    if (!JaasUtils.isZkSecurityEnabled(jaasFile)) {
      error("Security isn't enabled, most likely the file isn't set properly: %s".format(jaasFile))
      throw new IllegalArgumentException("Incorrect configuration") 
    }

    val zkUrl = options.valueOf(zkUrlOpt)

    /* TODO: Need to fix this before it is checked in, not compiling
     * val zkSessionTimeout = if (options.has(zkSessionTimeoutOpt)) {
       options.valueOf(zkSessionTimeoutOpt).intValue
     } else {
       30000
     }
     val zkConnectionTimeout = if (options.has(zkConnectionTimeoutOpt)) {
       options.valueOf(zkConnectionTimeoutOpt).intValue
     } else {
       30000
     }*/
    val zkUtils = ZkUtils.apply(zkUrl, 30000, 30000, true)

    for (path <- zkUtils.persistentZkPaths) {
      zkUtils.makeSurePersistentPathExists(path)
      setAclsRecursively(zkUtils, path)
    }

    while(counter.get > 0 && exception != null) {
      Thread.sleep(100)
    }

    if(exception != null) {
      throw exception
    }
  }

  def main(args: Array[String]) {
    try{
      run(args)
    } catch {
        case e: Exception =>
          e.printStackTrace()
    }
  }
  
  def main(args: Array[String]) {
      run(args)
  }
}