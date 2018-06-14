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
import kafka.server.KafkaConfig
import kafka.utils.{Logging, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.resource.ResourceNameType.{LITERAL, PREFIXED}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.{Before, Test}

class AclCommandTest extends ZooKeeperTestHarness with Logging {

  private val principal: KafkaPrincipal = KafkaPrincipal.fromString("User:test2")
  private val Users = Set(KafkaPrincipal.fromString("User:CN=writeuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown"),
    principal,
    KafkaPrincipal.fromString("""User:CN=\#User with special chars in CN : (\, \+ \" \\ \< \> \; ')"""))
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

  @Before
  override def setUp(): Unit = {
    super.setUp()

    brokerProps = TestUtils.createBrokerConfig(0, zkConnect)
    brokerProps.put(KafkaConfig.AuthorizerClassNameProp, "kafka.security.auth.SimpleAclAuthorizer")

    zkArgs = Array("--authorizer-properties", "zookeeper.connect=" + zkConnect)
  }

  @Test
  def testAclCli() {
    for ((resources, resourceCmd) <- ResourceToCommand) {
      for (permissionType <- PermissionType.values) {
        val operationToCmd = ResourceToOperations(resources)
        val (acls, cmd) = getAclToCommand(permissionType, operationToCmd._1)
          AclCommand.main(zkArgs ++ cmd ++ resourceCmd ++ operationToCmd._2 :+ "--add")
          for (resource <- resources) {
            withAuthorizer() { authorizer =>
              TestUtils.waitAndVerifyAcls(acls, authorizer, resource)
            }
          }

          testRemove(resources, resourceCmd, brokerProps)
      }
    }
  }

  @Test
  def testProducerConsumerCli() {
    for ((cmd, resourcesToAcls) <- CmdToResourcesToAcl) {
      val resourceCommand: Array[String] = resourcesToAcls.keys.map(ResourceToCommand).foldLeft(Array[String]())(_ ++ _)
      AclCommand.main(zkArgs ++ getCmd(Allow) ++ resourceCommand ++ cmd :+ "--add")
      for ((resources, acls) <- resourcesToAcls) {
        for (resource <- resources) {
          withAuthorizer() { authorizer =>
            TestUtils.waitAndVerifyAcls(acls, authorizer, resource)
          }
        }
      }
      testRemove(resourcesToAcls.keys.flatten.toSet, resourceCommand ++ cmd, brokerProps)
    }
  }

  @Test
  def testAclsOnPrefixedResources(): Unit = {
    val cmd = Array("--allow-principal", principal.toString, "--producer", "--topic", "Test-", "--resource-name-type", "Prefixed")

    AclCommand.main(zkArgs ++ cmd :+ "--add")

    withAuthorizer() { authorizer =>
      val writeAcl = Acl(principal, Allow, Acl.WildCardHost, Write)
      val describeAcl = Acl(principal, Allow, Acl.WildCardHost, Describe)
      val createAcl = Acl(principal, Allow, Acl.WildCardHost, Create)
      TestUtils.waitAndVerifyAcls(Set(writeAcl, describeAcl, createAcl), authorizer, Resource(Topic, "Test-", PREFIXED))
    }

    AclCommand.main(zkArgs ++ cmd :+ "--remove" :+ "--force")

    withAuthorizer() { authorizer =>
      TestUtils.waitAndVerifyAcls(Set.empty[Acl], authorizer, Resource(Cluster, "kafka-cluster", LITERAL))
      TestUtils.waitAndVerifyAcls(Set.empty[Acl], authorizer, Resource(Topic, "Test-", PREFIXED))
    }
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidAuthorizerProperty() {
    val args = Array("--authorizer-properties", "zookeeper.connect " + zkConnect)
    AclCommand.withAuthorizer(new AclCommandOptions(args))(null)
  }

  private def testRemove(resources: Set[Resource], resourceCmd: Array[String], brokerProps: Properties) {
    for (resource <- resources) {
      AclCommand.main(zkArgs ++ resourceCmd :+ "--remove" :+ "--force")
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

  def withAuthorizer()(f: Authorizer => Unit) {
    val kafkaConfig = KafkaConfig.fromProps(brokerProps, doLog = false)
    val authZ = new SimpleAclAuthorizer
    try {
      authZ.configure(kafkaConfig.originals)
      f(authZ)
    } finally authZ.close()
  }
}
