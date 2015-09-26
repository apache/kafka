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
package unit.kafka.admin

import java.util.Properties

import kafka.admin.AclCommand
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils.{Logging, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.{After, Assert, Test}

class AclCommandTest extends ZooKeeperTestHarness with Logging {

  private val resourceToCommand = Map[Set[Resource], Array[String]](
    Set(new Resource(Topic, "test-1"), new Resource(Topic, "test-2")) -> Array("--topic", "test-1, test-2"),
    Set(Resource.ClusterResource) -> Array("--cluster"),
    Set(new Resource(ConsumerGroup, "test-1"), new Resource(ConsumerGroup, "test-2")) -> Array("--consumer-group", "test-1, test-2")
  )

  private val operations: Set[Operation] = Set(Read, Write)
  private val operationsString = operations.mkString(AclCommand.delimiter)
  private val users = Set(KafkaPrincipal.fromString("User:test1"), KafkaPrincipal.fromString("User:test2"))
  private val usersString = users.mkString(AclCommand.delimiter)
  private val hosts = Set("host1", "host2")
  private val hostsString = hosts.mkString(AclCommand.delimiter)

  @Test
  def testAclCli() {
    val brokerProps = TestUtils.createBrokerConfig(0, zkConnect)
    brokerProps.put(KafkaConfig.AuthorizerClassNameProp, "kafka.security.auth.SimpleAclAuthorizer")

    val AllowAclsAndCommandOptions = getAclToCommand(Allow, operations)
    val DenyAclsAndCommandOptions = getAclToCommand(Deny, operations)
    val AllowAndDenyAclsAndCommandOptions = (AllowAclsAndCommandOptions._1 ++ DenyAclsAndCommandOptions._1,
      AllowAclsAndCommandOptions._2 ++ DenyAclsAndCommandOptions._2)
    val aclsToCmd = Set(AllowAclsAndCommandOptions, DenyAclsAndCommandOptions, AllowAndDenyAclsAndCommandOptions)

    val args = Array("--authorizer-properties", "zookeeper.connect=" + zkConnect, "--operations", operationsString)

    //now test add acls for each type of resource and with only allow, only deny and both allow and deny options.
    for ((resources, resourceCmd) <- resourceToCommand) {
      for ((acls, cmd) <- aclsToCmd) {
        AclCommand.main(args ++ cmd ++ resourceCmd :+ "--add")
        for (resource <- resources) {
          Assert.assertEquals(acls, getAuthorizer(brokerProps).getAcls(resource))

          //cleanup
          getAuthorizer(brokerProps).removeAcls(resource)
          Assert.assertEquals(Set.empty[Acl], getAuthorizer(brokerProps).getAcls(resource))
        }
      }
    }

    //The cleanup could have been done using --remove option instead of calling removeAcls directly on authorizer
    //but System.setIn and Console.setIn both do not work in scala-2.11 so this test gets stuck trying to read from stdin.
    //val in = new ByteArrayInputStream("y".getBytes())
    //System.setIn(in)
    //AclCommand.main(args :+ "--remove")
    //assertTrue(authorizer.getAcls(new Resource(Topic, topic)).isEmpty)
  }

  @Test
  def testProducerConsumerCli() {
    val brokerProps = TestUtils.createBrokerConfig(0, zkConnect)
    brokerProps.put(KafkaConfig.AuthorizerClassNameProp, "kafka.security.auth.SimpleAclAuthorizer")

    val args = Array("--authorizer-properties", "zookeeper.connect=" + zkConnect)
    val resourceCommand = resourceToCommand.values.foldLeft(Array[String]())((l, r) => l ++ r)

    AclCommand.main(args ++ resourceCommand ++ getAclToCommand(Allow, operations)._2 :+ "--add" :+ "--producer" :+ "--consumer")

    for (resources <- resourceToCommand.keys) {
      for (resource <- resources) {
        if (resource.resourceType == Topic)
          Assert.assertEquals(AclCommand.getAcls(users, Allow, Set[Operation](Write, Read, Describe), hosts), getAuthorizer(brokerProps).getAcls(resource))
        else if (resource.resourceType == ConsumerGroup)
          Assert.assertEquals(AclCommand.getAcls(users, Allow, Set[Operation](Read, Describe), hosts), getAuthorizer(brokerProps).getAcls(resource))
        else
          Assert.assertEquals(AclCommand.getAcls(users, Allow, Set[Operation](Create, Describe), hosts), getAuthorizer(brokerProps).getAcls(resource))

        //cleanup
        getAuthorizer(brokerProps).removeAcls(resource)
        Assert.assertEquals(Set.empty[Acl], getAuthorizer(brokerProps).getAcls(resource))
      }
    }
  }

  private def getAclToCommand(permissionType: PermissionType, operations: Set[Operation]): (Set[Acl], Array[String]) = {
    (AclCommand.getAcls(users, permissionType, operations, hosts),
      if (permissionType == Allow)
        Array("--allow-principals", usersString,
          "--allow-hosts", hostsString)
      else
        Array("--deny-principals", usersString,
          "--deny-hosts", hostsString))
  }

  def getAuthorizer(props: Properties): Authorizer = {
    val kafkaConfig = KafkaConfig.fromProps(props)
    val authZ = new SimpleAclAuthorizer
    authZ.configure(kafkaConfig.originals)

    authZ
  }

  @After
  def after() {
    //System.setIn(System.in)
  }
}