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
import scala.concurrent.duration._
import scala.util._

object ZkSecurityMigrator extends Logging {
  val usageMessage = ("ZooKeeper Migration Tool Help. This tool updates the ACLs of "
                      + "znodes as part of the process of setting up ZooKeeper "
                      + "authentication.")
  val workQueue = new LinkedBlockingQueue[Runnable]
  val threadExecutionContext = 
    ExecutionContext.fromExecutor(new ThreadPoolExecutor(1,
                                                         Runtime.getRuntime().availableProcessors(),
                                                         5000,
                                                         TimeUnit.MILLISECONDS,
                                                         workQueue))
  var futures: List[Future[String]] = List()
  var exception: ZkException = null
  var zkUtils: ZkUtils = null
  
  def setAclsRecursively(path: String) {
    val setPromise = Promise[String]
    val childrenPromise = Promise[String]
    futures :+ setPromise.future
    futures :+ childrenPromise.future
    zkUtils.zkConnection.getZookeeper.setACL(path, ZkUtils.DefaultAcls(true), -1, SetACLCallback, setPromise)
    zkUtils.zkConnection.getZookeeper.getChildren(path, false, GetChildrenCallback, childrenPromise)
  }

  object GetChildrenCallback extends ChildrenCallback {
      def processResult(rc: Int,
                        path: String,
                        ctx: Object,
                        children: java.util.List[String]) {
        val list = Option(children).map(_.asScala)
        val zkHandle = zkUtils.zkConnection.getZookeeper
        val promise = ctx.asInstanceOf[Promise[String]]
        Code.get(rc) match {
          case Code.OK =>
            // Set ACL for each child 
              Future {
                val childPathBuilder = new StringBuilder
                list.foreach(child => {
                  childPathBuilder.clear
                  childPathBuilder.append(path)
                  childPathBuilder.append("/")
                  childPathBuilder.append(child)

                  val childPath = childPathBuilder.toString
                  setAclsRecursively(childPath)
                })
              }(threadExecutionContext)
              promise success "done"
          case Code.CONNECTIONLOSS =>
            Future {
              zkHandle.getChildren(path, false, GetChildrenCallback, ctx)
            }(threadExecutionContext)
          case Code.NONODE =>
            warn("Node is gone, it could be have been legitimately deleted: %s".format(path))
          case Code.SESSIONEXPIRED =>
            // Starting a new session isn't really a problem, but it'd complicate
            // the logic of the tool, so we quit and let the user re-run it.
            error("ZooKeeper session expired while changing ACLs")
            promise failure ZkException.create(KeeperException.create(Code.get(rc)))
          case _ =>
            error("Unexpected return code: %d".format(rc))
            promise failure ZkException.create(KeeperException.create(Code.get(rc)))
        }
      }
  }
  
  object SetACLCallback extends StatCallback {
    def processResult(rc: Int,
                      path: String,
                      ctx: Object,
                      stat: Stat) {
      val zkHandle = zkUtils.zkConnection.getZookeeper
      val promise = ctx.asInstanceOf[Promise[String]]
      
      Code.get(rc) match {
          case Code.OK =>
            info("Successfully set ACLs for %s".format(path))
            promise success "done"
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
            promise failure ZkException.create(KeeperException.create(Code.get(rc)))
          case _ =>
            error("Unexpected return code: %d".format(rc))
            promise failure ZkException.create(KeeperException.create(Code.get(rc)))   
      }
    }
  }

  def run(args: Array[String]) {
    var jaasFile = System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    val parser = new OptionParser()

    val goOpt = parser.accepts("kafka.go", "Indicates whether to make this cluster secure or unsecure. The "
        + " options are 'secure' and 'unsecure'").withRequiredArg().ofType(classOf[String])
    val jaasFileOpt = parser.accepts("jaas.file", "JAAS Config file.").withOptionalArg().ofType(classOf[String])
    val zkUrlOpt = parser.accepts("zookeeper.connect", "Sets the ZooKeeper connect string (ensemble). This parameter " +
      "takes a comma-separated list of host:port pairs.").withRequiredArg().defaultsTo("localhost:2181").
      ofType(classOf[String])
    val zkSessionTimeoutOpt = parser.accepts("zookeeper.session.timeout", "Sets the ZooKeeper session timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[java.lang.Integer])
    val zkConnectionTimeoutOpt = parser.accepts("zookeeper.connection.timeout", "Sets the ZooKeeper connection timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[java.lang.Integer])
    val helpOpt = parser.accepts("help", "Print usage information.")

    val options = parser.parse(args : _*)
    if (options.has(helpOpt))
      CommandLineUtils.printUsageAndDie(parser, usageMessage)

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

    var goSecure: Boolean = options.valueOf(goOpt) match {
      case "secure" =>
        true
      case "unsecure" =>
        false
      case _ =>
        CommandLineUtils.printUsageAndDie(parser, usageMessage)
    }
    val zkUrl = options.valueOf(zkUrlOpt)
    val zkSessionTimeout = options.valueOf(zkSessionTimeoutOpt).intValue
    val zkConnectionTimeout = options.valueOf(zkConnectionTimeoutOpt).intValue
    zkUtils = ZkUtils(zkUrl, zkSessionTimeout, zkConnectionTimeout, goSecure)

    for (path <- zkUtils.securePersistentZkPaths) {
        zkUtils.makeSurePersistentPathExists(path)
        setAclsRecursively(path)
    }

    while(futures.size > 0) {
      val head::tail = futures
      Await.result(head, Duration.Inf)
      head.value match {
        case Some(Success(v)) => // nothing to do
        case Some(Failure(e)) => throw e
      }
    }
  }

  def main(args: Array[String]) {
    try {
      run(args)
    } catch {
        case e: Exception =>
          e.printStackTrace()
    }
  }
}