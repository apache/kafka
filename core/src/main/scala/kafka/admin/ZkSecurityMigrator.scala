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

import joptsimple.{OptionSet, OptionSpec, OptionSpecBuilder}
import kafka.server.KafkaConfig
import kafka.utils.{Exit, Logging, ToolsUtils}
import kafka.utils.Implicits._
import kafka.zk.{ControllerZNode, KafkaZkClient, ZkData, ZkSecurityMigratorUtils}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.server.util.{CommandDefaultOptions, CommandLineUtils}
import org.apache.zookeeper.AsyncCallback.{ChildrenCallback, StatCallback}
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.client.ZKClientConfig
import org.apache.zookeeper.data.Stat

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._
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
 *   ./bin/zookeeper-security-migration.sh --help
 * to see the configuration parameters. An example of running it is the following:
 *  ./bin/zookeeper-security-migration.sh --zookeeper.acl=secure --zookeeper.connect=localhost:2181
 * 
 * To convert a cluster from secure to unsecure, we need to perform the following
 * steps:
 * 1- Perform a rolling upgrade setting zookeeper.set.acl to false for each server
 * 2- Run this migration tool, setting zookeeper.acl to unsecure
 * 3- Perform another rolling upgrade to remove the system property setting the
 * login file (java.security.auth.login.config).
 */

object ZkSecurityMigrator extends Logging {
  private val usageMessage = ("ZooKeeper Migration Tool Help. This tool updates the ACLs of "
                      + "znodes as part of the process of setting up ZooKeeper "
                      + "authentication.")
  private val tlsConfigFileOption = "zk-tls-config-file"

  def run(args: Array[String]): Unit = {
    val jaasFile = System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    val opts = new ZkSecurityMigratorOptions(args)

    CommandLineUtils.maybePrintHelpOrVersion(opts, usageMessage)

    // Must have either SASL or TLS mutual authentication enabled to use this tool.
    // Instantiate the client config we will use so that we take into account config provided via the CLI option
    // and system properties passed via -D parameters if no CLI option is given.
    val zkClientConfig = createZkClientConfigFromOption(opts.options, opts.zkTlsConfigFile).getOrElse(new ZKClientConfig())
    val tlsClientAuthEnabled = KafkaConfig.zkTlsClientAuthEnabled(zkClientConfig)
    if (jaasFile == null && !tlsClientAuthEnabled) {
      val errorMsg = s"No JAAS configuration file has been specified and no TLS client certificate has been specified. Please make sure that you set " +
        s"the system property ${JaasUtils.JAVA_LOGIN_CONFIG_PARAM} or provide a ZooKeeper client TLS configuration via --$tlsConfigFileOption <filename> " +
        s"identifying at least ${KafkaConfig.ZkSslClientEnableProp}, ${KafkaConfig.ZkClientCnxnSocketProp}, and ${KafkaConfig.ZkSslKeyStoreLocationProp}"
      System.err.println("ERROR: %s".format(errorMsg))
      throw new IllegalArgumentException("Incorrect configuration")
    }

    if (!tlsClientAuthEnabled && !JaasUtils.isZkSaslEnabled) {
      val errorMsg = "Security isn't enabled, most likely the file isn't set properly: %s".format(jaasFile)
      System.out.println("ERROR: %s".format(errorMsg))
      throw new IllegalArgumentException("Incorrect configuration")
    }

    val zkAcl = opts.options.valueOf(opts.zkAclOpt) match {
      case "secure" =>
        info("zookeeper.acl option is secure")
        true
      case "unsecure" =>
        info("zookeeper.acl option is unsecure")
        false
      case _ =>
        ToolsUtils.printUsageAndExit(opts.parser, usageMessage)
    }
    val zkUrl = opts.options.valueOf(opts.zkUrlOpt)
    val zkSessionTimeout = opts.options.valueOf(opts.zkSessionTimeoutOpt).intValue
    val zkConnectionTimeout = opts.options.valueOf(opts.zkConnectionTimeoutOpt).intValue
    val zkClient = KafkaZkClient(zkUrl, zkAcl, zkSessionTimeout, zkConnectionTimeout,
      Int.MaxValue, Time.SYSTEM, zkClientConfig = zkClientConfig, name = "ZkSecurityMigrator")
    val enablePathCheck = opts.options.has(opts.enablePathCheckOpt)
    val migrator = new ZkSecurityMigrator(zkClient)
    migrator.run(enablePathCheck)
  }

  def main(args: Array[String]): Unit = {
    try {
      run(args)
    } catch {
        case e: Exception =>
          e.printStackTrace()
          // must exit with non-zero status so system tests will know we failed
          Exit.exit(1)
    }
  }

  def createZkClientConfigFromFile(filename: String) : ZKClientConfig = {
    val zkTlsConfigFileProps = Utils.loadProps(filename, KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.toList.asJava)
    val zkClientConfig = new ZKClientConfig() // Initializes based on any system properties that have been set
    // Now override any set system properties with explicitly-provided values from the config file
    // Emit INFO logs due to camel-case property names encouraging mistakes -- help people see mistakes they make
    info(s"Found ${zkTlsConfigFileProps.size()} ZooKeeper client configuration properties in file $filename")
    zkTlsConfigFileProps.asScala.forKeyValue { (key, value) =>
      info(s"Setting $key")
      KafkaConfig.setZooKeeperClientProperty(zkClientConfig, key, value)
    }
    zkClientConfig
  }

  private[admin] def createZkClientConfigFromOption(options: OptionSet, option: OptionSpec[String]) : Option[ZKClientConfig] =
    if (!options.has(option))
      None
    else
      Some(createZkClientConfigFromFile(options.valueOf(option)))

  private class ZkSecurityMigratorOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val zkAclOpt: OptionSpec[String] = parser.accepts("zookeeper.acl", "Indicates whether to make the Kafka znodes in ZooKeeper secure or unsecure."
      + " The options are 'secure' and 'unsecure'").withRequiredArg().ofType(classOf[String])
    val zkUrlOpt: OptionSpec[String] = parser.accepts("zookeeper.connect", "Sets the ZooKeeper connect string (ensemble). This parameter " +
      "takes a comma-separated list of host:port pairs.").withRequiredArg().defaultsTo("localhost:2181").
      ofType(classOf[String])
    val zkSessionTimeoutOpt: OptionSpec[Integer] = parser.accepts("zookeeper.session.timeout", "Sets the ZooKeeper session timeout.").
      withRequiredArg().ofType(classOf[java.lang.Integer]).defaultsTo(30000)
    val zkConnectionTimeoutOpt: OptionSpec[Integer] = parser.accepts("zookeeper.connection.timeout", "Sets the ZooKeeper connection timeout.").
      withRequiredArg().ofType(classOf[java.lang.Integer]).defaultsTo(30000)
    val enablePathCheckOpt: OptionSpecBuilder = parser.accepts("enable.path.check", "Checks if all the root paths exist in ZooKeeper " +
      "before migration. If not, exit the command.")
    val zkTlsConfigFile: OptionSpec[String] = parser.accepts(tlsConfigFileOption,
      "Identifies the file where ZooKeeper client TLS connectivity properties are defined.  Any properties other than " +
        KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.mkString(", ") + " are ignored.")
      .withRequiredArg().describedAs("ZooKeeper TLS configuration").ofType(classOf[String])
    options = parser.parse(args : _*)
  }
}

class ZkSecurityMigrator(zkClient: KafkaZkClient) extends Logging {
  private val zkSecurityMigratorUtils = new ZkSecurityMigratorUtils(zkClient)
  private val futures = new mutable.Queue[Future[String]]

  private def setAcl(path: String, setPromise: Promise[String]): Unit = {
    info("Setting ACL for path %s".format(path))
    zkSecurityMigratorUtils.currentZooKeeper.setACL(path, zkClient.defaultAcls(path).asJava, -1, SetACLCallback, setPromise)
  }

  private def retrieveChildren(path: String, childrenPromise: Promise[String]): Unit = {
    info("Getting children to set ACLs for path %s".format(path))
    zkSecurityMigratorUtils.currentZooKeeper.getChildren(path, false, GetChildrenCallback, childrenPromise)
  }

  private def setAclIndividually(path: String): Unit = {
    val setPromise = Promise[String]()
    futures.synchronized {
      futures += setPromise.future
    }
    setAcl(path, setPromise)
  }

  private def setAclsRecursively(path: String): Unit = {
    val setPromise = Promise[String]()
    val childrenPromise = Promise[String]()
    futures.synchronized {
      futures += setPromise.future
      futures += childrenPromise.future
    }
    setAcl(path, setPromise)
    retrieveChildren(path, childrenPromise)
  }

  private object GetChildrenCallback extends ChildrenCallback {
    def processResult(rc: Int,
                      path: String,
                      ctx: Object,
                      children: java.util.List[String]): Unit = {
      val zkHandle = zkSecurityMigratorUtils.currentZooKeeper
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
          promise failure KeeperException.create(Code.get(rc))
        case _ =>
          System.out.println("Unexpected return code: %d".format(rc))
          promise failure KeeperException.create(Code.get(rc))
      }
    }
  }

  private object SetACLCallback extends StatCallback {
    def processResult(rc: Int,
                      path: String,
                      ctx: Object,
                      stat: Stat): Unit = {
      val zkHandle = zkSecurityMigratorUtils.currentZooKeeper
      val promise = ctx.asInstanceOf[Promise[String]]

      Code.get(rc) match {
        case Code.OK =>
          info("Successfully set ACLs for %s".format(path))
          promise success "done"
        case Code.CONNECTIONLOSS =>
            zkHandle.setACL(path, zkClient.defaultAcls(path).asJava, -1, SetACLCallback, ctx)
        case Code.NONODE =>
          warn("Znode is gone, it could be have been legitimately deleted: %s".format(path))
          promise success "done"
        case Code.SESSIONEXPIRED =>
          // Starting a new session isn't really a problem, but it'd complicate
          // the logic of the tool, so we quit and let the user re-run it.
          System.out.println("ZooKeeper session expired while changing ACLs")
          promise failure KeeperException.create(Code.get(rc))
        case _ =>
          System.out.println("Unexpected return code: %d".format(rc))
          promise failure KeeperException.create(Code.get(rc))
      }
    }
  }

  private def run(enablePathCheck: Boolean): Unit = {
    try {
      setAclIndividually("/")
      checkPathExistenceAndMaybeExit(enablePathCheck)
      for (path <- ZkData.SecureRootPaths) {
        debug("Going to set ACL for %s".format(path))
        if (path == ControllerZNode.path && !zkClient.pathExists(path)) {
          debug("Ignoring to set ACL for %s, because it doesn't exist".format(path))
        } else {
          zkClient.makeSurePersistentPathExists(path)
          setAclsRecursively(path)
        }
      }

      @tailrec
      def recurse(): Unit = {
        val future = futures.synchronized { 
          futures.headOption
        }
        future match {
          case Some(a) =>
            Await.result(a, 6000 millis)
            futures.synchronized { futures.dequeue() }
            recurse()
          case None =>
        }
      }
      recurse()

    } finally {
      zkClient.close()
    }
  }

  private def checkPathExistenceAndMaybeExit(enablePathCheck: Boolean): Unit = {
    val nonExistingSecureRootPaths = ZkData.SecureRootPaths.filterNot(zkClient.pathExists)
    if (nonExistingSecureRootPaths.nonEmpty) {
      println(s"Warning: The following secure root paths do not exist in ZooKeeper: ${nonExistingSecureRootPaths.mkString(",")}")
      println("That might be due to an incorrect chroot is specified when executing the command.")
      if (enablePathCheck) {
        println("Exit the command.")
        // must exit with non-zero status so system tests will know we failed
        Exit.exit(1)
      }
    }
  }
}
