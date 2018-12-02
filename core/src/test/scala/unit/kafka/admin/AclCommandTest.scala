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

import java.util.Properties

import kafka.admin.AclCommand.AclCommandOptions
import kafka.security.auth._
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{Exit, Logging, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.network.ListenerName

import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.SecurityUtils
import org.junit.{After, Before, Test}

class AclCommandTest extends ZooKeeperTestHarness with Logging {

  var servers: Seq[KafkaServer] = Seq()

  private val principal: KafkaPrincipal = SecurityUtils.parseKafkaPrincipal("User:test2")
  private val Users = Set(SecurityUtils.parseKafkaPrincipal("User:CN=writeuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown"),
    principal, SecurityUtils.parseKafkaPrincipal("""User:CN=\#User with special chars in CN : (\, \+ \" \\ \< \> \; ')"""))
  private val Hosts = Set("host1", "host2")
  private val AllowHostCommand = Array("--allow-host", "host1", "--allow-host", "host2")
  private val DenyHostCommand = Array("--deny-host", "host1", "--deny-host", "host2")

  private val TopicResources = Set(Resource(Topic, "test-1", LITERAL), Resource(Topic, "test-2", LITERAL))
  private val GroupResources = Set(Resource(Group, "testGroup-1", LITERAL), Resource(Group, "testGroup-2", LITERAL))
  private val TransactionalIdResources = Set(Resource(TransactionalId, "t0", LITERAL), Resource(TransactionalId, "t1", LITERAL))
  private val TokenResources = Set(Resource(DelegationToken, "token1", LITERAL), Resource(DelegationToken, "token2", LITERAL))

  private val ResourceToCommand = Map[Set[Resource], Array[String]](
    TopicResources -> Array("--topic", "test-1", "--topic", "test-2"),
    Set(Resource.ClusterResource) -> Array("--cluster"),
    GroupResources -> Array("--group", "testGroup-1", "--group", "testGroup-2"),
    TransactionalIdResources -> Array("--transactional-id", "t0", "--transactional-id", "t1"),
    TokenResources -> Array("--delegation-token", "token1", "--delegation-token", "token2")
  )

  private val ResourceToOperations = Map[Set[Resource], (Set[Operation], Array[String])](
    TopicResources -> (Set(Read, Write, Create, Describe, Delete, DescribeConfigs, AlterConfigs),
      Array("--operation", "Read" , "--operation", "Write", "--operation", "Create", "--operation", "Describe", "--operation", "Delete",
        "--operation", "DescribeConfigs", "--operation", "AlterConfigs")),
    Set(Resource.ClusterResource) -> (Set(Create, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite),
      Array("--operation", "Create", "--operation", "ClusterAction", "--operation", "DescribeConfigs",
        "--operation", "AlterConfigs", "--operation", "IdempotentWrite")),
    GroupResources -> (Set(Read, Describe), Array("--operation", "Read", "--operation", "Describe")),
    TransactionalIdResources -> (Set(Describe, Write), Array("--operation", "Describe", "--operation", "Write")),
    TokenResources -> (Set(Describe), Array("--operation", "Describe"))
  )

  private def ProducerResourceToAcls(enableIdempotence: Boolean = false) = Map[Set[Resource], Set[Acl]](
    TopicResources -> AclCommand.getAcls(Users, Allow, Set(Write, Describe, Create), Hosts),
    TransactionalIdResources -> AclCommand.getAcls(Users, Allow, Set(Write, Describe), Hosts),
    Set(Resource.ClusterResource) -> AclCommand.getAcls(Users, Allow,
      Set(if (enableIdempotence) Some(IdempotentWrite) else None).flatten, Hosts)
  )

  private val ConsumerResourceToAcls = Map[Set[Resource], Set[Acl]](
    TopicResources -> AclCommand.getAcls(Users, Allow, Set(Read, Describe), Hosts),
    GroupResources -> AclCommand.getAcls(Users, Allow, Set(Read), Hosts)
  )

  private val CmdToResourcesToAcl = Map[Array[String], Map[Set[Resource], Set[Acl]]](
    Array[String]("--producer") -> ProducerResourceToAcls(),
    Array[String]("--producer", "--idempotent") -> ProducerResourceToAcls(enableIdempotence = true),
    Array[String]("--consumer") -> ConsumerResourceToAcls,
    Array[String]("--producer", "--consumer") -> ConsumerResourceToAcls.map { case (k, v) => k -> (v ++
      ProducerResourceToAcls().getOrElse(k, Set.empty[Acl])) },
    Array[String]("--producer", "--idempotent", "--consumer") -> ConsumerResourceToAcls.map { case (k, v) => k -> (v ++
      ProducerResourceToAcls(enableIdempotence = true).getOrElse(k, Set.empty[Acl])) }
  )

  private var brokerProps: Properties = _
  private var zkArgs: Array[String] = _
  private var adminArgs: Array[String] = _

  @Before
  override def setUp(): Unit = {
    super.setUp()

    brokerProps = TestUtils.createBrokerConfig(0, zkConnect)
    brokerProps.put(KafkaConfig.AuthorizerClassNameProp, "kafka.security.auth.SimpleAclAuthorizer")
    brokerProps.put(SimpleAclAuthorizer.SuperUsersProp, "User:ANONYMOUS")

    zkArgs = Array("--authorizer-properties", "zookeeper.connect=" + zkConnect)
  }

  @After
  override def tearDown() {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testAclCliWithAuthorizer(): Unit = {
    testAclCli(zkArgs)
  }

  @Test
  def testAclCliWithAdminAPI(): Unit = {
    createServer()
    testAclCli(adminArgs)
  }

  private def createServer(): Unit = {
    servers = Seq(TestUtils.createServer(KafkaConfig.fromProps(brokerProps)))
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    adminArgs = Array("--bootstrap-server", TestUtils.bootstrapServers(servers, listenerName))
  }

  private def testAclCli(cmdArgs: Array[String]) {
    for ((resources, resourceCmd) <- ResourceToCommand) {
      for (permissionType <- PermissionType.values) {
        val operationToCmd = ResourceToOperations(resources)
        val (acls, cmd) = getAclToCommand(permissionType, operationToCmd._1)
          AclCommand.main(cmdArgs ++ cmd ++ resourceCmd ++ operationToCmd._2 :+ "--add")
          for (resource <- resources) {
            withAuthorizer() { authorizer =>
              TestUtils.waitAndVerifyAcls(acls, authorizer, resource)
            }
          }

          testRemove(cmdArgs, resources, resourceCmd)
      }
    }
  }

  @Test
  def testProducerConsumerCliWithAuthorizer(): Unit = {
    testProducerConsumerCli(zkArgs)
  }

  @Test
  def testProducerConsumerCliWithAdminAPI(): Unit = {
    createServer()
    testProducerConsumerCli(adminArgs)
  }

  private def testProducerConsumerCli(cmdArgs: Array[String]) {
    for ((cmd, resourcesToAcls) <- CmdToResourcesToAcl) {
      val resourceCommand: Array[String] = resourcesToAcls.keys.map(ResourceToCommand).foldLeft(Array[String]())(_ ++ _)
      AclCommand.main(cmdArgs ++ getCmd(Allow) ++ resourceCommand ++ cmd :+ "--add")
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

  @Test
  def testAclsOnPrefixedResourcesWithAdminAPI(): Unit = {
    createServer()
    testAclsOnPrefixedResources(adminArgs)
  }

  private def testAclsOnPrefixedResources(cmdArgs: Array[String]): Unit = {
    val cmd = Array("--allow-principal", principal.toString, "--producer", "--topic", "Test-", "--resource-pattern-type", "Prefixed")

    AclCommand.main(cmdArgs ++ cmd :+ "--add")

    withAuthorizer() { authorizer =>
      val writeAcl = Acl(principal, Allow, Acl.WildCardHost, Write)
      val describeAcl = Acl(principal, Allow, Acl.WildCardHost, Describe)
      val createAcl = Acl(principal, Allow, Acl.WildCardHost, Create)
      TestUtils.waitAndVerifyAcls(Set(writeAcl, describeAcl, createAcl), authorizer, Resource(Topic, "Test-", PREFIXED))
    }

    AclCommand.main(cmdArgs ++ cmd :+ "--remove" :+ "--force")

    withAuthorizer() { authorizer =>
      TestUtils.waitAndVerifyAcls(Set.empty[Acl], authorizer, Resource(Cluster, "kafka-cluster", LITERAL))
      TestUtils.waitAndVerifyAcls(Set.empty[Acl], authorizer, Resource(Topic, "Test-", PREFIXED))
    }
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidAuthorizerProperty() {
    val args = Array("--authorizer-properties", "zookeeper.connect " + zkConnect)
    val aclCommandService = new AclCommand.AuthorizerService(new AclCommandOptions(args))
    aclCommandService.listAcls()
  }

  @Test
  def testPatternTypes() {
    Exit.setExitProcedure { (status, _) =>
      if (status == 1)
        throw new RuntimeException("Exiting command")
      else
        throw new AssertionError(s"Unexpected exit with status $status")
    }
    def verifyPatternType(cmd: Array[String], isValid: Boolean): Unit = {
      if (isValid)
        AclCommand.main(cmd)
      else
        intercept[RuntimeException](AclCommand.main(cmd))
    }
    try {
      PatternType.values.foreach { patternType =>
        val addCmd = zkArgs ++ Array("--allow-principal", principal.toString, "--producer", "--topic", "Test",
          "--add", "--resource-pattern-type", patternType.toString)
        verifyPatternType(addCmd, isValid = patternType.isSpecific)
        val listCmd = zkArgs ++ Array("--topic", "Test", "--list", "--resource-pattern-type", patternType.toString)
        verifyPatternType(listCmd, isValid = patternType != PatternType.UNKNOWN)
        val removeCmd = zkArgs ++ Array("--topic", "Test", "--force", "--remove", "--resource-pattern-type", patternType.toString)
        verifyPatternType(removeCmd, isValid = patternType != PatternType.UNKNOWN)
      }
    } finally {
      Exit.resetExitProcedure()
    }
  }

  private def testRemove(cmdArgs: Array[String], resources: Set[Resource], resourceCmd: Array[String]) {
    for (resource <- resources) {
      AclCommand.main(cmdArgs ++ resourceCmd :+ "--remove" :+ "--force")
      withAuthorizer() { authorizer =>
        TestUtils.waitAndVerifyAcls(Set.empty[Acl], authorizer, resource)
      }
    }
  }

  private def getAclToCommand(permissionType: PermissionType, operations: Set[Operation]): (Set[Acl], Array[String]) = {
    (AclCommand.getAcls(Users, permissionType, operations, Hosts), getCmd(permissionType))
  }

  private def getCmd(permissionType: PermissionType): Array[String] = {
    val principalCmd = if (permissionType == Allow) "--allow-principal" else "--deny-principal"
    val cmd = if (permissionType == Allow) AllowHostCommand else DenyHostCommand

    Users.foldLeft(cmd) ((cmd, user) => cmd ++ Array(principalCmd, user.toString))
  }

  private def withAuthorizer()(f: Authorizer => Unit) {
    val kafkaConfig = KafkaConfig.fromProps(brokerProps, doLog = false)
    val authZ = new SimpleAclAuthorizer
    try {
      authZ.configure(kafkaConfig.originals)
      f(authZ)
    } finally authZ.close()
  }
}
