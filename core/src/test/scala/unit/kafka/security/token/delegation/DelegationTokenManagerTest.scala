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

package kafka.security.token.delegation

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.Properties

import kafka.network.RequestChannel.Session
import kafka.security.auth.Acl.WildCardHost
import kafka.security.auth._
import kafka.server.{CreateTokenResult, Defaults, DelegationTokenManager, KafkaConfig}
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.scram.internal.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internal.DelegationTokenCache
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.{MockTime, SecurityUtils}
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class DelegationTokenManagerTest extends ZooKeeperTestHarness  {

  val time = new MockTime()
  val owner = SecurityUtils.parseKafkaPrincipal("User:owner")
  val renewer = List(SecurityUtils.parseKafkaPrincipal("User:renewer1"))

  val masterKey = "masterKey"
  val maxLifeTimeMsDefault = Defaults.DelegationTokenMaxLifeTimeMsDefault
  val renewTimeMsDefault = Defaults.DelegationTokenExpiryTimeMsDefault
  var tokenCache: DelegationTokenCache = null
  var props: Properties = null

  var createTokenResult: CreateTokenResult = _
  var error: Errors = Errors.NONE
  var expiryTimeStamp: Long = 0

  @Before
  override def setUp() {
    super.setUp()
    props = TestUtils.createBrokerConfig(0, zkConnect, enableToken = true)
    props.put(KafkaConfig.SaslEnabledMechanismsProp, ScramMechanism.mechanismNames().asScala.mkString(","))
    props.put(KafkaConfig.DelegationTokenMasterKeyProp, masterKey)
    tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames())
  }

  @Test
  def testTokenRequestsWithDelegationTokenDisabled(): Unit = {
    val props: Properties = TestUtils.createBrokerConfig(0, zkConnect)
    val config = KafkaConfig.fromProps(props)
    val tokenManager = new DelegationTokenManager(config, tokenCache, time, zkClient)

    tokenManager.createToken(owner, renewer, -1, createTokenResultCallBack)
    assertEquals(Errors.DELEGATION_TOKEN_AUTH_DISABLED, createTokenResult.error)
    assert(Array[Byte]() sameElements createTokenResult.hmac)

    tokenManager.renewToken(owner, ByteBuffer.wrap("test".getBytes), 1000000, renewResponseCallback)
    assertEquals(Errors.DELEGATION_TOKEN_AUTH_DISABLED, error)

    tokenManager.expireToken(owner, ByteBuffer.wrap("test".getBytes), 1000000, renewResponseCallback)
    assertEquals(Errors.DELEGATION_TOKEN_AUTH_DISABLED, error)
  }

  @Test
  def testCreateToken(): Unit = {
    val config = KafkaConfig.fromProps(props)
    val tokenManager = new DelegationTokenManager(config, tokenCache, time, zkClient)
    tokenManager.startup

    tokenManager.createToken(owner, renewer, -1 , createTokenResultCallBack)
    val issueTime = time.milliseconds
    val tokenId = createTokenResult.tokenId
    val password = DelegationTokenManager.createHmac(tokenId, masterKey)
    assertEquals(CreateTokenResult(issueTime, issueTime + renewTimeMsDefault,  issueTime + maxLifeTimeMsDefault, tokenId, password, Errors.NONE), createTokenResult)

    val token = tokenManager.getToken(tokenId)
    assertTrue(!token.isEmpty )
    assertTrue(password sameElements token.get.hmac)
  }

  @Test
  def testRenewToken(): Unit = {
    val config = KafkaConfig.fromProps(props)
    val tokenManager = new DelegationTokenManager(config, tokenCache, time, zkClient)
    tokenManager.startup

    tokenManager.createToken(owner, renewer, -1 , createTokenResultCallBack)
    val issueTime = time.milliseconds
    val maxLifeTime = issueTime + maxLifeTimeMsDefault
    val tokenId = createTokenResult.tokenId
    val password = DelegationTokenManager.createHmac(tokenId, masterKey)
    assertEquals(CreateTokenResult(issueTime, issueTime + renewTimeMsDefault,  maxLifeTime, tokenId, password, Errors.NONE), createTokenResult)

    //try renewing non-existing token
    tokenManager.renewToken(owner, ByteBuffer.wrap("test".getBytes), -1 , renewResponseCallback)
    assertEquals(Errors.DELEGATION_TOKEN_NOT_FOUND, error)

    //try renew non-owned tokens
    val unknownOwner = SecurityUtils.parseKafkaPrincipal("User:Unknown")
    tokenManager.renewToken(unknownOwner, ByteBuffer.wrap(password), -1 , renewResponseCallback)
    assertEquals(Errors.DELEGATION_TOKEN_OWNER_MISMATCH, error)

    // try renew with default time period
    time.sleep(24 * 60 * 60 * 1000L)
    var expectedExpiryStamp = time.milliseconds + renewTimeMsDefault
    tokenManager.renewToken(owner, ByteBuffer.wrap(password), -1 , renewResponseCallback)
    assertEquals(expectedExpiryStamp, expiryTimeStamp)
    assertEquals(Errors.NONE, error)

    // try renew with specific time period
    time.sleep(24 * 60 * 60 * 1000L)
    expectedExpiryStamp = time.milliseconds + 1 * 60 * 60 * 1000L
    tokenManager.renewToken(owner, ByteBuffer.wrap(password), 1 * 60 * 60 * 1000L , renewResponseCallback)
    assertEquals(expectedExpiryStamp, expiryTimeStamp)
    assertEquals(Errors.NONE, error)

    //try renewing more than max time period
    time.sleep( 1 * 60 * 60 * 1000L)
    tokenManager.renewToken(owner, ByteBuffer.wrap(password), 8 * 24 * 60 * 60 * 1000L, renewResponseCallback)
    assertEquals(maxLifeTime, expiryTimeStamp)
    assertEquals(Errors.NONE, error)

    //try renewing expired token
    time.sleep(8 * 24 * 60 * 60 * 1000L)
    tokenManager.renewToken(owner, ByteBuffer.wrap(password), -1 , renewResponseCallback)
    assertEquals(Errors.DELEGATION_TOKEN_EXPIRED, error)
  }

  @Test
  def testExpireToken(): Unit = {
    val config = KafkaConfig.fromProps(props)
    val tokenManager = new DelegationTokenManager(config, tokenCache, time, zkClient)
    tokenManager.startup

    tokenManager.createToken(owner, renewer, -1 , createTokenResultCallBack)
    val issueTime = time.milliseconds
    val tokenId = createTokenResult.tokenId
    val password = DelegationTokenManager.createHmac(tokenId, masterKey)
    assertEquals(CreateTokenResult(issueTime, issueTime + renewTimeMsDefault,  issueTime + maxLifeTimeMsDefault, tokenId, password, Errors.NONE), createTokenResult)

    //try expire non-existing token
    tokenManager.expireToken(owner, ByteBuffer.wrap("test".getBytes), -1 , renewResponseCallback)
    assertEquals(Errors.DELEGATION_TOKEN_NOT_FOUND, error)

    //try expire non-owned tokens
    val unknownOwner = SecurityUtils.parseKafkaPrincipal("User:Unknown")
    tokenManager.expireToken(unknownOwner, ByteBuffer.wrap(password), -1 , renewResponseCallback)
    assertEquals(Errors.DELEGATION_TOKEN_OWNER_MISMATCH, error)

    //try expire token at a timestamp
    time.sleep(24 * 60 * 60 * 1000L)
    val expectedExpiryStamp = time.milliseconds + 2 * 60 * 60 * 1000L
    tokenManager.expireToken(owner, ByteBuffer.wrap(password), 2 * 60 * 60 * 1000L, renewResponseCallback)
    assertEquals(expectedExpiryStamp, expiryTimeStamp)

    //try expire token immediately
    time.sleep(1 * 60 * 60 * 1000L)
    tokenManager.expireToken(owner, ByteBuffer.wrap(password), -1, renewResponseCallback)
    assert(tokenManager.getToken(tokenId).isEmpty)
    assertEquals(Errors.NONE, error)
    assertEquals(time.milliseconds, expiryTimeStamp)
  }

  @Test
  def testDescribeToken(): Unit = {

    val config = KafkaConfig.fromProps(props)

    val owner1 = SecurityUtils.parseKafkaPrincipal("User:owner1")
    val owner2 = SecurityUtils.parseKafkaPrincipal("User:owner2")
    val owner3 = SecurityUtils.parseKafkaPrincipal("User:owner3")
    val owner4 = SecurityUtils.parseKafkaPrincipal("User:owner4")

    val renewer1 = SecurityUtils.parseKafkaPrincipal("User:renewer1")
    val renewer2 = SecurityUtils.parseKafkaPrincipal("User:renewer2")
    val renewer3 = SecurityUtils.parseKafkaPrincipal("User:renewer3")
    val renewer4 = SecurityUtils.parseKafkaPrincipal("User:renewer4")

    val simpleAclAuthorizer = new SimpleAclAuthorizer
    simpleAclAuthorizer.configure(config.originals)

    var hostSession = new Session(owner1, InetAddress.getByName("192.168.1.1"))

    val tokenManager = new DelegationTokenManager(config, tokenCache, time, zkClient)
    tokenManager.startup

    //create tokens
    tokenManager.createToken(owner1, List(renewer1, renewer2), 1 * 60 * 60 * 1000L, createTokenResultCallBack)

    tokenManager.createToken(owner2, List(renewer3), 1 * 60 * 60 * 1000L, createTokenResultCallBack)
    val tokenId2 = createTokenResult.tokenId

    tokenManager.createToken(owner3, List(renewer4), 2 * 60 * 60 * 1000L, createTokenResultCallBack)
    val tokenId3 = createTokenResult.tokenId

    tokenManager.createToken(owner4, List(owner1, renewer4), 2 * 60 * 60 * 1000L, createTokenResultCallBack)

    assert(tokenManager.getAllTokenInformation().size == 4 )

    //get tokens non-exiting owner
    var  tokens = getTokens(tokenManager, simpleAclAuthorizer, hostSession, owner1, List(SecurityUtils.parseKafkaPrincipal("User:unknown")))
    assert(tokens.size == 0)

    //get all tokens for  empty owner list
    tokens = getTokens(tokenManager, simpleAclAuthorizer, hostSession, owner1, List())
    assert(tokens.size == 0)

    //get all tokens for owner1
    tokens = getTokens(tokenManager, simpleAclAuthorizer, hostSession, owner1, List(owner1))
    assert(tokens.size == 2)

    //get all tokens for owner1
    tokens = getTokens(tokenManager, simpleAclAuthorizer, hostSession, owner1, null)
    assert(tokens.size == 2)

    //get all tokens for unknown owner
    tokens = getTokens(tokenManager, simpleAclAuthorizer, hostSession, SecurityUtils.parseKafkaPrincipal("User:unknown"), null)
    assert(tokens.size == 0)

    //get all tokens for multiple owners (owner1, renewer4) and without permission for renewer4
    tokens = getTokens(tokenManager, simpleAclAuthorizer, hostSession, owner1, List(owner1, renewer4))
    assert(tokens.size == 2)

    //get all tokens for multiple owners (owner1, renewer4) and with permission
    var acl = new Acl(owner1, Allow, WildCardHost, Describe)
    simpleAclAuthorizer.addAcls(Set(acl), new Resource(kafka.security.auth.DelegationToken, tokenId3))
    tokens = getTokens(tokenManager, simpleAclAuthorizer, hostSession, owner1, List(owner1, renewer4))
    assert(tokens.size == 3)

    //get all tokens for renewer4 which is a renewer principal for some tokens
    tokens = getTokens(tokenManager, simpleAclAuthorizer, hostSession,  renewer4, List(renewer4))
    assert(tokens.size == 2)

    //get all tokens for multiple owners (renewer2, renewer3) which are token renewers principals and without permissions for renewer3
    tokens = getTokens(tokenManager, simpleAclAuthorizer, hostSession,  renewer2, List(renewer2, renewer3))
    assert(tokens.size == 1)

    //get all tokens for multiple owners (renewer2, renewer3) which are token renewers principals and with permissions
    hostSession = new Session(renewer2, InetAddress.getByName("192.168.1.1"))
    acl = new Acl(renewer2, Allow, WildCardHost, Describe)
    simpleAclAuthorizer.addAcls(Set(acl), new Resource(kafka.security.auth.DelegationToken, tokenId2))
    tokens = getTokens(tokenManager, simpleAclAuthorizer, hostSession,  renewer2, List(renewer2, renewer3))
    assert(tokens.size == 2)

    simpleAclAuthorizer.close()
  }

  private def getTokens(tokenManager: DelegationTokenManager, simpleAclAuthorizer: SimpleAclAuthorizer, hostSession: Session,
                        requestPrincipal: KafkaPrincipal, requestedOwners: List[KafkaPrincipal]): List[DelegationToken] = {

    if (requestedOwners != null && requestedOwners.isEmpty) {
      List()
    }
    else {
      def authorizeToken(tokenId: String) = simpleAclAuthorizer.authorize(hostSession, Describe, new Resource(kafka.security.auth.DelegationToken, tokenId))
      def eligible(token: TokenInformation) = DelegationTokenManager.filterToken(requestPrincipal, Option(requestedOwners), token, authorizeToken)
      tokenManager.getTokens(eligible)
    }
  }

  @Test
  def testPeriodicTokenExpiry(): Unit = {
    val config = KafkaConfig.fromProps(props)
    val tokenManager = new DelegationTokenManager(config, tokenCache, time, zkClient)
    tokenManager.startup

    //create tokens
    tokenManager.createToken(owner, renewer, 1 * 60 * 60 * 1000L, createTokenResultCallBack)
    tokenManager.createToken(owner, renewer, 1 * 60 * 60 * 1000L, createTokenResultCallBack)
    tokenManager.createToken(owner, renewer, 2 * 60 * 60 * 1000L, createTokenResultCallBack)
    tokenManager.createToken(owner, renewer, 2 * 60 * 60 * 1000L, createTokenResultCallBack)
    assert(tokenManager.getAllTokenInformation().size == 4 )

    time.sleep(2 * 60 * 60 * 1000L)
    tokenManager.expireTokens()
    assert(tokenManager.getAllTokenInformation().size == 2 )

  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
  }

  private def createTokenResultCallBack(ret: CreateTokenResult): Unit = {
    createTokenResult = ret
  }

  private def renewResponseCallback(ret: Errors, timeStamp: Long): Unit = {
    error = ret
    expiryTimeStamp = timeStamp
  }
}
