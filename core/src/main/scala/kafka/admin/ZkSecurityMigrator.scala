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
import joptsimple.OptionParser
import org.I0Itec.zkclient.exception.ZkException
import kafka.utils.{Logging, ZkUtils, CommandLineUtils}
import org.apache.log4j.Level
import org.apache.kafka.common.security.JaasUtils
import org.apache.zookeeper.AsyncCallback.{ChildrenCallback, StatCallback}
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection._
import scala.collection.mutable.Queue
import scala.concurrent._
import scala.concurrent.duration._

/**
 * This tool is to be used when making access to ZooKeeper authenticated or 
 * the other way around, when removing authenticated access. The exact steps
 * to migrate a Kafka cluster from unsecure to secure with respect to ZooKeeper
 * access are the following:
 * 
 * 1- Perform a rolling upgrade of Kafka servers, setting zookeeper.set.acl to false
 * and passing a valid JAAS login file via the system property 
 * java.security.auth.login.config
 * 2- Perform a second rolling upgrade keeping the system property for the login file
 * and now setting zookeeper.set.acl to true
 * 3- Finally run this tool. There is a script under ./bin. Run 
 *   ./bin/zookeeper-security-migration --help
 * to see the configuration parameters. An example of running it is the following:
 *  ./bin/zookeeper-security-migration --zookeeper.acl=secure --zookeeper.connection=localhost:2181
 * 
 * To convert a cluster from secure to unsecure, we need to perform the following
 * steps:
 * 1- Perform a rolling upgrade setting zookeeper.set.acl to false for each server
 * 2- Run this migration tool, setting zookeeper.acl to unsecure
 * 3- Perform another rolling upgrade to remove the system property setting the
 * login file (java.security.auth.login.config).
 */

object ZkSecurityMigrator extends Logging {
  val usageMessage = ("ZooKeeper Migration Tool Help. This tool updates the ACLs of "
                      + "znodes as part of the process of setting up ZooKeeper "
                      + "authentication.")

  def run(args: Array[String]) {
    var jaasFile = System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    val parser = new OptionParser()
    val zkAclOpt = parser.accepts("zookeeper.acl", "Indicates whether to make the Kafka znodes in ZooKeeper secure or unsecure."
        + " The options are 'secure' and 'unsecure'").withRequiredArg().ofType(classOf[String])
    val zkUrlOpt = parser.accepts("zookeeper.connect", "Sets the ZooKeeper connect string (ensemble). This parameter " +
      "takes a comma-separated list of host:port pairs.").withRequiredArg().defaultsTo("localhost:2181").
      ofType(classOf[String])
    val zkSessionTimeoutOpt = parser.accepts("zookeeper.session.timeout", "Sets the ZooKeeper session timeout.").
      withRequiredArg().ofType(classOf[java.lang.Integer]).defaultsTo(30000)
    val zkConnectionTimeoutOpt = parser.accepts("zookeeper.connection.timeout", "Sets the ZooKeeper connection timeout.").
      withRequiredArg().ofType(classOf[java.lang.Integer]).defaultsTo(30000)
    val helpOpt = parser.accepts("help", "Print usage information.")

    val options = parser.parse(args : _*)
    if (options.has(helpOpt))
      CommandLineUtils.printUsageAndDie(parser, usageMessage)

    if ((jaasFile == null)) {
     val errorMsg = ("No JAAS configuration file has been specified. Please make sure that you have set " +
                    "the system property %s".format(JaasUtils.JAVA_LOGIN_CONFIG_PARAM))
     System.out.println("ERROR: %s".format(errorMsg))
     throw new IllegalArgumentException("Incorrect configuration")
    }

    if (!JaasUtils.isZkSecurityEnabled()) {
      val errorMsg = "Security isn't enabled, most likely the file isn't set properly: %s".format(jaasFile)
      System.out.println("ERROR: %s".format(errorMsg))
      throw new IllegalArgumentException("Incorrect configuration") 
    }

    val zkAcl: Boolean = options.valueOf(zkAclOpt) match {
      case "secure" =>
        info("zookeeper.acl option is secure")
        true
      case "unsecure" =>
        info("zookeeper.acl option is unsecure")
        false
      case _ =>
        CommandLineUtils.printUsageAndDie(parser, usageMessage)
    }
    val zkUrl = options.valueOf(zkUrlOpt)
    val zkSessionTimeout = options.valueOf(zkSessionTimeoutOpt).intValue
    val zkConnectionTimeout = options.valueOf(zkConnectionTimeoutOpt).intValue
    val zkUtils = ZkUtils(zkUrl, zkSessionTimeout, zkConnectionTimeout, zkAcl)
    val migrator = new ZkSecurityMigrator(zkUtils)
    migrator.run()
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

class ZkSecurityMigrator(zkUtils: ZkUtils) extends Logging {
  private val workQueue = new LinkedBlockingQueue[Runnable]
  private val futures = new Queue[Future[String]]

  private def setAcl(path: String, setPromise: Promise[String]) = {
    info("Setting ACL for path %s".format(path))
    zkUtils.zkConnection.getZookeeper.setACL(path, ZkUtils.DefaultAcls(zkUtils.isSecure), -1, SetACLCallback, setPromise)
  }

  private def getChildren(path: String, childrenPromise: Promise[String]) = {
    info("Getting children to set ACLs for path %s".format(path))
    zkUtils.zkConnection.getZookeeper.getChildren(path, false, GetChildrenCallback, childrenPromise)
  }

  private def setAclIndividually(path: String) = {
    val setPromise = Promise[String]
    futures.synchronized {
      futures += setPromise.future
    }
    setAcl(path, setPromise)
  }

  private def setAclsRecursively(path: String) = {
    val setPromise = Promise[String]
    val childrenPromise = Promise[String]
    futures.synchronized {
      futures += setPromise.future
      futures += childrenPromise.future
    }
    setAcl(path, setPromise)
    getChildren(path, childrenPromise)
  }

  private object GetChildrenCallback extends ChildrenCallback {
    def processResult(rc: Int,
                      path: String,
                      ctx: Object,
                      children: java.util.List[String]) {
      val zkHandle = zkUtils.zkConnection.getZookeeper
      val promise = ctx.asInstanceOf[Promise[String]]
      Code.get(rc) match {
        case Code.OK =>
          // Set ACL for each child
          children.asScala.map { child =>
            path match {
              case "/" => s"/$child"
              case path => s"$path/$child"
            }
          }.foreach(setAclsRecursively)
          promise success "done"
        case Code.CONNECTIONLOSS =>
          zkHandle.getChildren(path, false, GetChildrenCallback, ctx)
        case Code.NONODE =>
          warn("Node is gone, it could be have been legitimately deleted: %s".format(path))
          promise success "done"
        case Code.SESSIONEXPIRED =>
          // Starting a new session isn't really a problem, but it'd complicate
          // the logic of the tool, so we quit and let the user re-run it.
          System.out.println("ZooKeeper session expired while changing ACLs")
          promise failure ZkException.create(KeeperException.create(Code.get(rc)))
        case _ =>
          System.out.println("Unexpected return code: %d".format(rc))
          promise failure ZkException.create(KeeperException.create(Code.get(rc)))
      }
    }
  }

  private object SetACLCallback extends StatCallback {
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
            zkHandle.setACL(path, ZkUtils.DefaultAcls(zkUtils.isSecure), -1, SetACLCallback, ctx)
        case Code.NONODE =>
          warn("Znode is gone, it could be have been legitimately deleted: %s".format(path))
          promise success "done"
        case Code.SESSIONEXPIRED =>
          // Starting a new session isn't really a problem, but it'd complicate
          // the logic of the tool, so we quit and let the user re-run it.
          System.out.println("ZooKeeper session expired while changing ACLs")
          promise failure ZkException.create(KeeperException.create(Code.get(rc)))
        case _ =>
          System.out.println("Unexpected return code: %d".format(rc))
          promise failure ZkException.create(KeeperException.create(Code.get(rc)))
      }
    }
  }

  private def run(): Unit = {
    try {
      setAclIndividually("/")
      for (path <- zkUtils.securePersistentZkPaths) {
        debug("Going to set ACL for %s".format(path))
        zkUtils.makeSurePersistentPathExists(path)
        setAclsRecursively(path)
      }
      
      @tailrec
      def recurse(): Unit = {
        val future = futures.synchronized { 
          futures.headOption
        }
        future match {
          case Some(a) =>
            Await.result(a, 6000 millis)
            futures.synchronized { futures.dequeue }
            recurse
          case None =>
        }
      }
      recurse()

    } finally {
      zkUtils.close
    }
  }
}
