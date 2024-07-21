/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import kafka.admin.AclCommand.AclCommandOptions
import kafka.security.authorizer.AclAuthorizer
import kafka.server.{KafkaBroker, KafkaConfig, QuorumTestHarness}
import kafka.utils.{Exit, Logging, TestUtils}
import org.apache.kafka.common.acl.{AccessControlEntry, AclOperation, AclPermissionType}
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType._
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.{AppInfoParser, LogCaptureAppender, SecurityUtils}
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.config.ServerConfigs
import org.apache.log4j.Level
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.{ByteArrayOutputStream, File}
import java.util.Properties
import javax.management.InstanceAlreadyExistsException

class AclCommandTest extends QuorumTestHarness with Logging {

  var servers: Seq[KafkaBroker] = Seq()

  private val principal: KafkaPrincipal = SecurityUtils.parseKafkaPrincipal("User:test2")
  private val Users = Set(SecurityUtils.parseKafkaPrincipal("User:CN=writeuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown"),
    principal, SecurityUtils.parseKafkaPrincipal("""User:CN=\#User with special chars in CN : (\, \+ \" \\ \< \> \; ')"""))
  private val Hosts = Set("host1", "host2")
  private val AllowHostCommand = Array("--allow-host", "host1", "--allow-host", "host2")
  private val DenyHostCommand = Array("--deny-host", "host1", "--deny-host", "host2")

  private val ClusterResource = new ResourcePattern(CLUSTER, Resource.CLUSTER_NAME, LITERAL)
  private val TopicResources = Set(new ResourcePattern(TOPIC, "test-1", LITERAL), new ResourcePattern(TOPIC, "test-2", LITERAL))
  private val GroupResources = Set(new ResourcePattern(GROUP, "testGroup-1", LITERAL), new ResourcePattern(GROUP, "testGroup-2", LITERAL))
  private val TransactionalIdResources = Set(new ResourcePattern(TRANSACTIONAL_ID, "t0", LITERAL), new ResourcePattern(TRANSACTIONAL_ID, "t1", LITERAL))
  private val TokenResources = Set(new ResourcePattern(DELEGATION_TOKEN, "token1", LITERAL), new ResourcePattern(DELEGATION_TOKEN, "token2", LITERAL))
  private val UserResources = Set(new ResourcePattern(USER, "User:test-user1", LITERAL), new ResourcePattern(USER, "User:test-user2", LITERAL))

  private val ResourceToCommand = Map[Set[ResourcePattern], Array[String]](
    TopicResources -> Array("--topic", "test-1", "--topic", "test-2"),
    Set(ClusterResource) -> Array("--cluster"),
    GroupResources -> Array("--group", "testGroup-1", "--group", "testGroup-2"),
    TransactionalIdResources -> Array("--transactional-id", "t0", "--transactional-id", "t1"),
    TokenResources -> Array("--delegation-token", "token1", "--delegation-token", "token2"),
    UserResources -> Array("--user-principal", "User:test-user1", "--user-principal", "User:test-user2")
  )

  private val ResourceToOperations = Map[Set[ResourcePattern], (Set[AclOperation], Array[String])](
    TopicResources -> (Set(READ, WRITE, CREATE, DESCRIBE, DELETE, DESCRIBE_CONFIGS, ALTER_CONFIGS, ALTER),
      Array("--operation", "Read" , "--operation", "Write", "--operation", "Create", "--operation", "Describe", "--operation", "Delete",
        "--operation", "DescribeConfigs", "--operation", "AlterConfigs", "--operation", "Alter")),
    Set(ClusterResource) -> (Set(CREATE, CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, ALTER, DESCRIBE),
      Array("--operation", "Create", "--operation", "ClusterAction", "--operation", "DescribeConfigs",
        "--operation", "AlterConfigs", "--operation", "IdempotentWrite", "--operation", "Alter", "--operation", "Describe")),
    GroupResources -> (Set(READ, DESCRIBE, DELETE), Array("--operation", "Read", "--operation", "Describe", "--operation", "Delete")),
    TransactionalIdResources -> (Set(DESCRIBE, WRITE), Array("--operation", "Describe", "--operation", "Write")),
    TokenResources -> (Set(DESCRIBE), Array("--operation", "Describe")),
    UserResources -> (Set(CREATE_TOKENS, DESCRIBE_TOKENS), Array("--operation", "CreateTokens", "--operation", "DescribeTokens"))
  )

  private def ProducerResourceToAcls(enableIdempotence: Boolean = false) = Map[Set[ResourcePattern], Set[AccessControlEntry]](
    TopicResources -> AclCommand.getAcls(Users, ALLOW, Set(WRITE, DESCRIBE, CREATE), Hosts),
    TransactionalIdResources -> AclCommand.getAcls(Users, ALLOW, Set(WRITE, DESCRIBE), Hosts),
    Set(ClusterResource) -> AclCommand.getAcls(Users, ALLOW,
      Set(if (enableIdempotence) Some(IDEMPOTENT_WRITE) else None).flatten, Hosts)
  )

  private val ConsumerResourceToAcls = Map[Set[ResourcePattern], Set[AccessControlEntry]](
    TopicResources -> AclCommand.getAcls(Users, ALLOW, Set(READ, DESCRIBE), Hosts),
    GroupResources -> AclCommand.getAcls(Users, ALLOW, Set(READ), Hosts)
  )

  private val CmdToResourcesToAcl = Map[Array[String], Map[Set[ResourcePattern], Set[AccessControlEntry]]](
    Array[String]("--producer") -> ProducerResourceToAcls(),
    Array[String]("--producer", "--idempotent") -> ProducerResourceToAcls(enableIdempotence = true),
    Array[String]("--consumer") -> ConsumerResourceToAcls,
    Array[String]("--producer", "--consumer") -> ConsumerResourceToAcls.map { case (k, v) => k -> (v ++
      ProducerResourceToAcls().getOrElse(k, Set.empty[AccessControlEntry])) },
    Array[String]("--producer", "--idempotent", "--consumer") -> ConsumerResourceToAcls.map { case (k, v) => k -> (v ++
      ProducerResourceToAcls(enableIdempotence = true).getOrElse(k, Set.empty[AccessControlEntry])) }
  )

  private var brokerProps: Properties = _
  private var zkArgs: Array[String] = _
  private var adminArgs: Array[String] = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    brokerProps = TestUtils.createBrokerConfig(0, zkConnectOrNull)
    if (isKRaftTest()) {
      brokerProps.putAll(kraftControllerConfigs().head)
    } else {
      brokerProps.put(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, classOf[AclAuthorizer].getName)
      brokerProps.put(AclAuthorizer.SuperUsersProp, "User:ANONYMOUS")
      zkArgs = Array("--authorizer-properties", "zookeeper.connect=" + zkConnect)
    }
  }

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  override protected def kraftControllerConfigs(): Seq[Properties] = {
    val controllerConfig = new Properties
    controllerConfig.put(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, classOf[StandardAuthorizer].getName)
    controllerConfig.put(StandardAuthorizer.SUPER_USERS_CONFIG, "User:ANONYMOUS")
    Seq(controllerConfig)
  }

  @Test
  def testAclCliWithAuthorizer(): Unit = {
    testAclCli(zkArgs)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAclCliWithAdminAPI(quorum: String): Unit = {
    createServer()
    testAclCli(adminArgs)
  }

  private def createServer(commandConfig: Option[File] = None): Unit = {
    servers = Seq(createBroker(KafkaConfig.fromProps(brokerProps)))
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)

    var adminArgs = Array("--bootstrap-server", TestUtils.bootstrapServers(servers, listenerName))
    if (commandConfig.isDefined) {
      adminArgs ++= Array("--command-config", commandConfig.get.getAbsolutePath)
    }
    this.adminArgs = adminArgs
  }

  private def callMain(args: Array[String]): (String, String) = {
    grabConsoleOutputAndError(AclCommand.main(args))
  }

  private def testAclCli(cmdArgs: Array[String]): Unit = {
    for ((resources, resourceCmd) <- ResourceToCommand) {
      for (permissionType <- Set(ALLOW, DENY)) {
        val operationToCmd = ResourceToOperations(resources)
        val (acls, cmd) = getAclToCommand(permissionType, operationToCmd._1)
        val (addOut, addErr) = callMain(cmdArgs ++ cmd ++ resourceCmd ++ operationToCmd._2 :+ "--add")
        assertOutputContains("Adding ACLs", resources, resourceCmd, addOut)
        assertEquals("", addErr)

        for (resource <- resources) {
          withAuthorizer() { authorizer =>
            TestUtils.waitAndVerifyAcls(acls, authorizer, resource)
          }
        }

        val (listOut, listErr) = callMain(cmdArgs :+ "--list")
        assertOutputContains("Current ACLs", resources, resourceCmd, listOut)
        assertEquals("", listErr)

        testRemove(cmdArgs, resources, resourceCmd)
      }
    }
  }

  private def assertOutputContains(prefix: String, resources: Set[ResourcePattern], resourceCmd: Array[String], output: String): Unit = {
    resources.foreach { resource =>
      val resourceType = resource.resourceType.toString
      (if (resource == ClusterResource) Array("kafka-cluster") else resourceCmd.filterNot(_.startsWith("--"))).foreach { name =>
        val expected = s"$prefix for resource `ResourcePattern(resourceType=$resourceType, name=$name, patternType=LITERAL)`:"
        assertTrue(output.contains(expected), s"Substring $expected not in output:\n$output")
      }
    }
  }

  @Test
  def testProducerConsumerCliWithAuthorizer(): Unit = {
    testProducerConsumerCli(zkArgs)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testProducerConsumerCliWithAdminAPI(quorum: String): Unit = {
    createServer()
    testProducerConsumerCli(adminArgs)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAclCliWithClientId(quorum: String): Unit = {
    val adminClientConfig = TestUtils.tempFile("client.id=my-client")

    createServer(Some(adminClientConfig))

    val appender = LogCaptureAppender.createAndRegister()
    appender.setClassLogger(classOf[AppInfoParser], Level.WARN)
    try {
      testAclCli(adminArgs)
    } finally {
      appender.close()
    }
    assertEquals(0, appender.getEvents.stream()
      .filter(e => e.getLevel == Level.WARN.toString)
      .filter(_.getThrowableClassName.filter(_ ==classOf[InstanceAlreadyExistsException].getName).isPresent)
      .count(), "There should be no warnings about multiple registration of mbeans")
  }

  private def testProducerConsumerCli(cmdArgs: Array[String]): Unit = {
    for ((cmd, resourcesToAcls) <- CmdToResourcesToAcl) {
      val resourceCommand: Array[String] = resourcesToAcls.keys.map(ResourceToCommand).foldLeft(Array[String]())(_ ++ _)
      callMain(cmdArgs ++ getCmd(ALLOW) ++ resourceCommand ++ cmd :+ "--add")
      for ((resources, acls) <- resourcesToAcls) {
        for (resource <- resources) {
          withAuthorizer() { authorizer =>
            TestUtils.waitAndVerifyAcls(acls, authorizer, resource)
          }
        }
      }
      testRemove(cmdArgs, resourcesToAcls.keys.flatten.toSet, resourceCommand ++ cmd)
    }
  }

  @Test
  def testAclsOnPrefixedResourcesWithAuthorizer(): Unit = {
    testAclsOnPrefixedResources(zkArgs)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAclsOnPrefixedResourcesWithAdminAPI(quorum: String): Unit = {
    createServer()
    testAclsOnPrefixedResources(adminArgs)
  }

  private def testAclsOnPrefixedResources(cmdArgs: Array[String]): Unit = {
    val cmd = Array("--allow-principal", principal.toString, "--producer", "--topic", "Test-", "--resource-pattern-type", "Prefixed")

    callMain(cmdArgs ++ cmd :+ "--add")

    withAuthorizer() { authorizer =>
      val writeAcl = new AccessControlEntry(principal.toString, AclEntry.WILDCARD_HOST, WRITE, ALLOW)
      val describeAcl = new AccessControlEntry(principal.toString, AclEntry.WILDCARD_HOST, DESCRIBE, ALLOW)
      val createAcl = new AccessControlEntry(principal.toString, AclEntry.WILDCARD_HOST, CREATE, ALLOW)
      TestUtils.waitAndVerifyAcls(Set(writeAcl, describeAcl, createAcl), authorizer,
        new ResourcePattern(TOPIC, "Test-", PREFIXED))
    }

    callMain(cmdArgs ++ cmd :+ "--remove" :+ "--force")

    withAuthorizer() { authorizer =>
      TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], authorizer, new ResourcePattern(CLUSTER, "kafka-cluster", LITERAL))
      TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], authorizer, new ResourcePattern(TOPIC, "Test-", PREFIXED))
    }
  }

  @Test
  def testInvalidAuthorizerProperty(): Unit = {
    val args = Array("--authorizer-properties", "zookeeper.connect " + zkConnect)
    val aclCommandService = new AclCommand.AuthorizerService(classOf[AclAuthorizer].getName,
      new AclCommandOptions(args))
    assertThrows(classOf[IllegalArgumentException], () => aclCommandService.listAcls())
  }

  @Test
  def testPatternTypesWithAuthorizer(): Unit = {
    testPatternTypes(zkArgs)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testPatternTypesWithAdminAPI(quorum: String): Unit = {
    createServer()
    testPatternTypes(adminArgs)
  }

  private def testPatternTypes(cmdArgs: Array[String]): Unit = {
    Exit.setExitProcedure { (status, _) =>
      if (status == 1)
        throw new RuntimeException("Exiting command")
      else
        throw new AssertionError(s"Unexpected exit with status $status")
    }
    def verifyPatternType(cmd: Array[String], isValid: Boolean): Unit = {
      if (isValid)
        callMain(cmd)
      else
        assertThrows(classOf[RuntimeException], () => callMain(cmd))
    }
    try {
      PatternType.values.foreach { patternType =>
        val addCmd = cmdArgs ++ Array("--allow-principal", principal.toString, "--producer", "--topic", "Test",
          "--add", "--resource-pattern-type", patternType.toString)
        verifyPatternType(addCmd, isValid = patternType.isSpecific)
        val listCmd = cmdArgs ++ Array("--topic", "Test", "--list", "--resource-pattern-type", patternType.toString)
        verifyPatternType(listCmd, isValid = patternType != PatternType.UNKNOWN)
        val removeCmd = cmdArgs ++ Array("--topic", "Test", "--force", "--remove", "--resource-pattern-type", patternType.toString)
        verifyPatternType(removeCmd, isValid = patternType != PatternType.UNKNOWN)
      }
    } finally {
      Exit.resetExitProcedure()
    }
  }

  private def testRemove(cmdArgs: Array[String], resources: Set[ResourcePattern], resourceCmd: Array[String]): Unit = {
    val (out, err) = callMain(cmdArgs ++ resourceCmd :+ "--remove" :+ "--force")
    assertEquals("", err)
    for (resource <- resources) {
      withAuthorizer() { authorizer =>
        TestUtils.waitAndVerifyAcls(Set.empty[AccessControlEntry], authorizer, resource)
      }
    }
  }

  private def getAclToCommand(permissionType: AclPermissionType, operations: Set[AclOperation]): (Set[AccessControlEntry], Array[String]) = {
    (AclCommand.getAcls(Users, permissionType, operations, Hosts), getCmd(permissionType))
  }

  private def getCmd(permissionType: AclPermissionType): Array[String] = {
    val principalCmd = if (permissionType == ALLOW) "--allow-principal" else "--deny-principal"
    val cmd = if (permissionType == ALLOW) AllowHostCommand else DenyHostCommand

    Users.foldLeft(cmd) ((cmd, user) => cmd ++ Array(principalCmd, user.toString))
  }

  private def withAuthorizer()(f: Authorizer => Unit): Unit = {
    if (isKRaftTest()) {
      (servers.map(_.authorizer.get) ++ controllerServers.map(_.authorizer.get)).foreach { auth =>
        f(auth)
      }
    } else {
      val kafkaConfig = KafkaConfig.fromProps(brokerProps, doLog = false)
      val auth = new AclAuthorizer
      try {
        auth.configure(kafkaConfig.originals)
        f(auth)
      } finally auth.close()
    }
  }

  /**
   * Capture both the console output and console error during the execution of the provided function.
   */
  private def grabConsoleOutputAndError(f: => Unit) : (String, String) = {
    val out = new ByteArrayOutputStream
    val err = new ByteArrayOutputStream
    try scala.Console.withOut(out)(scala.Console.withErr(err)(f))
    finally {
      scala.Console.out.flush()
      scala.Console.err.flush()
    }
    (out.toString, err.toString)
  }
}
