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
package kafka.security.auth

import java.net.InetAddress
import java.util.UUID
import java.net.InetAddress
import java.util.UUID

import kafka.network.RequestChannel.Session
import kafka.security.auth.Acl.WildCardHost
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.Assert._
import org.junit.{After, Before, Test}

class SimpleRegexAclAuthorizerTest extends ZooKeeperTestHarness {

  val simpleAclAuthorizer = new SimpleRegexAclAuthorizer
  val simpleAclAuthorizer2 = new SimpleRegexAclAuthorizer
  val testPrincipal = Acl.WildCardPrincipal
  val testHostName: InetAddress = InetAddress.getByName("192.168.0.1")
  val session = new Session(testPrincipal, testHostName)
  var resource: Resource = _
  val superUsers = "User:superuser1; User:superuser2"
  val username = "alice"
  var config: KafkaConfig = _

  @Before
  override def setUp() {
    super.setUp()

    // Increase maxUpdateRetries to avoid transient failures
    simpleAclAuthorizer.maxUpdateRetries = Int.MaxValue
    simpleAclAuthorizer2.maxUpdateRetries = Int.MaxValue

    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(SimpleAclAuthorizer.SuperUsersProp, superUsers)

    config = KafkaConfig.fromProps(props)
    simpleAclAuthorizer.configure(config.originals)
    simpleAclAuthorizer2.configure(config.originals)
    resource = new Resource(Topic, s"regex-${UUID.randomUUID().toString}")
  }

  @After
  override def tearDown(): Unit = {
    simpleAclAuthorizer.close()
    simpleAclAuthorizer2.close()
  }

  @Test
  def testTopicWithRegexAcls(): Unit = {
    assertFalse("when acls = [],  authorizer should fail close.", simpleAclAuthorizer.authorize(session, Read, resource))

    val user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val host1 = InetAddress.getByName("192.168.3.1")
    val readAcl = new Acl(user1, Allow, host1.getHostAddress, Read)
    val regexResource = new Resource(resource.resourceType, "r:regex-.*")

    val acls = changeAclAndVerify(Set.empty[Acl], Set[Acl](readAcl), Set.empty[Acl], regexResource)

    val host1Session = new Session(user1, host1)
    assertTrue("User1 should have Read access from host1", simpleAclAuthorizer.authorize(host1Session, Read, resource))

    //allow Write to specific topic.
    val writeAcl = new Acl(user1, Allow, host1.getHostAddress, Write)
    changeAclAndVerify(Set.empty[Acl], Set[Acl](readAcl, writeAcl), Set.empty[Acl])

    //deny Write to wild card topic.
    val denyWriteOnWildCardResourceAcl = new Acl(user1, Deny, host1.getHostAddress, Write)
    changeAclAndVerify(acls, Set[Acl](denyWriteOnWildCardResourceAcl), Set.empty[Acl], regexResource)

    // expected acls Set(User:alice has Allow permission for operations: Read from hosts: 192.168.3.1, User:alice has Deny permission for operations: Write from hosts: 192.168.3.1)
    //       but got Set(User:alice has Deny permission for operations: Write from hosts: 192.168.3.1)
    assertFalse("User1 should not have Write access from host1", simpleAclAuthorizer.authorize(host1Session, Write, resource))
  }

  private def changeAclAndVerify(originalAcls: Set[Acl], addedAcls: Set[Acl], removedAcls: Set[Acl], resource: Resource = resource): Set[Acl] = {
    var acls = originalAcls

    if(addedAcls.nonEmpty) {
      simpleAclAuthorizer.addAcls(addedAcls, resource)
      acls ++= addedAcls
    }

    if(removedAcls.nonEmpty) {
      simpleAclAuthorizer.removeAcls(removedAcls, resource)
      acls --=removedAcls
    }

    TestUtils.waitAndVerifyAcls(acls, simpleAclAuthorizer, resource)

    acls
  }

}
