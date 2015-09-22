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

import java.io._
import java.util.Properties

import kafka.admin.AclCommand
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils.{Logging, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.{After, Test, Assert}

class AclCommandTest extends ZooKeeperTestHarness with Logging {

  private val resourceToCommand = Map[Resource, Array[String]](
    new Resource(Topic, "test") -> Array("--topic", "test"),
    Resource.ClusterResource -> Array("--cluster"),
    new Resource(ConsumerGroup, "test") -> Array("--consumer-group", "test")
  )

  private val operations: Set[Operation] = Set(Read, Write)
  private val operationsString = operations.mkString(AclCommand.delimeter)
  private val users = Set(KafkaPrincipal.fromString("User:test1"), KafkaPrincipal.fromString("User:test2"))
  private val usersString = users.mkString(AclCommand.delimeter)
  private val hosts = Set("host1", "host2")
  private val hostsString = hosts.mkString(AclCommand.delimeter)

  @Test
  def testAclCli() {
    val brokerProps: Properties = TestUtils.createBrokerConfig(0, zkConnect)
    brokerProps.put(KafkaConfig.AuthorizerClassNameProp, "kafka.security.auth.SimpleAclAuthorizer")

    //create a temp server.properties file
    val config: File = TestUtils.tempFile()
    brokerProps.store(new FileWriter(config), "test-broker-config")

    val AllowAclsAndCommandOptions = getAclToCommand(Allow)
    val DenyAclsAndCommandOptions = getAclToCommand(Deny)
    val AllowAndDenyAclsAndCommandOptions = (AllowAclsAndCommandOptions._1 ++ DenyAclsAndCommandOptions._1,
      AllowAclsAndCommandOptions._2 ++ DenyAclsAndCommandOptions._2)
    val aclsToCmd = Set(AllowAclsAndCommandOptions, DenyAclsAndCommandOptions, AllowAndDenyAclsAndCommandOptions)

    val args: Array[String] = Array("--config", config.getPath,"--operations", operationsString)

    //now test add acls for each type of resource and with only allow, only deny and both allow and deny options.
    for((resource, resourceCmd) <- resourceToCommand) {
      for((acls, cmd) <- aclsToCmd) {
        AclCommand.main(args ++ cmd ++ resourceCmd :+ "--add")
        Assert.assertEquals(acls, getAuthorizer(brokerProps).getAcls(resource))

        //cleanup
        getAuthorizer(brokerProps).removeAcls(resource)
        Assert.assertEquals(Set.empty[Acl], getAuthorizer(brokerProps).getAcls(resource))
      }
    }

    //The cleanup could have been done using --remove option instead of calling removeAcls directly on authorizer
    //but System.setIn and Console.setIn both do not work in scala-2.11 so this test gets stuck trying to read from stdin.
    //val in = new ByteArrayInputStream("y".getBytes())
    //System.setIn(in)
    //AclCommand.main(args :+ "--remove")
    //assertTrue(authorizer.getAcls(new Resource(Topic, topic)).isEmpty)
  }

  private def getAclToCommand(permissionType: PermissionType) : (Set[Acl], Array[String]) = {
    (AclCommand.getAcls(users, permissionType, operations, hosts),
      if(permissionType == Allow)
        Array("--allowprincipals", usersString,
        "--allowhosts", hostsString)
      else
        Array("--denyprincipals", usersString,
          "--denyhosts", hostsString))
  }

  def getAuthorizer(props: Properties): Authorizer = {
    val kafkaConfig = KafkaConfig.fromProps(props)
    val authZ: SimpleAclAuthorizer = new SimpleAclAuthorizer
    authZ.configure(kafkaConfig.originals)

    authZ
  }

  @After
  def after(): Unit = {
    //System.setIn(System.in)
  }
}