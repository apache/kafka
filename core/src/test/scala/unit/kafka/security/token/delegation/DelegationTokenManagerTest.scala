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
import java.util.{Base64, Properties}
import kafka.network.RequestChannel.Session
import kafka.security.authorizer.{AclAuthorizer, AuthorizerUtils}
import kafka.security.authorizer.AclEntry.WildcardHost
import kafka.server.{CreateTokenResult, Defaults, DelegationTokenManager, KafkaConfig, QuorumTestHarness}
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding, AclOperation}
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType.ALLOW
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType.{DELEGATION_TOKEN, USER}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.{MockTime, SecurityUtils, Time}
import org.apache.kafka.server.authorizer._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.Buffer

class DelegationTokenManagerTest extends QuorumTestHarness  {

  val time = new MockTime()
  val owner = SecurityUtils.parseKafkaPrincipal("User:owner")
  val renewer = List(SecurityUtils.parseKafkaPrincipal("User:renewer1"))
  val tokenManagers = Buffer[DelegationTokenManager]()

  val secretKey = "secretKey"
  val maxLifeTimeMsDefault = Defaults.DelegationTokenMaxLifeTimeMsDefault
  val renewTimeMsDefault = Defaults.DelegationTokenExpiryTimeMsDefault
  var tokenCache: DelegationTokenCache = _
  var props: Properties = _

  var createTokenResult: CreateTokenResult = _
  var error: Errors = Errors.NONE
  var expiryTimeStamp: Long = 0

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    props = TestUtils.createBrokerConfig(0, zkConnect, enableToken = true)
    props.put(KafkaConfig.SaslEnabledMechanismsProp, ScramMechanism.mechanismNames().asScala.mkString(","))
    props.put(KafkaConfig.DelegationTokenSecretKeyProp, secretKey)
    tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames())
  }

  @AfterEach
  override def tearDown(): Unit = {
    tokenManagers.foreach(_.shutdown())
    super.tearDown()
  }

  @Test
  def testTokenRequestsWithDelegationTokenDisabled(): Unit = {
    val props: Properties = TestUtils.createBrokerConfig(0, zkConnect)
    val config = KafkaConfig.fromProps(props)
    val tokenManager = createDelegationTokenManager(config, tokenCache, time, zkClient)

    tokenManager.createToken(owner, owner, renewer, -1, createTokenResultCallBack)
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
    val tokenManager = createDelegationTokenManager(config, tokenCache, time, zkClient)
    tokenManager.startup()

    tokenManager.createToken(owner, owner, renewer, -1 , createTokenResultCallBack)
    val issueTime = time.milliseconds
    val tokenId = createTokenResult.tokenId
    val password = DelegationTokenManager.createHmac(tokenId, secretKey)
    assertEquals(CreateTokenResult(owner, owner, issueTime, issueTime + renewTimeMsDefault,  issueTime + maxLifeTimeMsDefault, tokenId, password, Errors.NONE), createTokenResult)

    val token = tokenManager.getToken(tokenId)
    assertFalse(token.isEmpty )
    assertTrue(password sameElements token.get.hmac)
  }

  @Test
  def testRenewToken(): Unit = {
    val config = KafkaConfig.fromProps(props)
    val tokenManager = createDelegationTokenManager(config, tokenCache, time, zkClient)
    tokenManager.startup()

    tokenManager.createToken(owner, owner, renewer, -1 , createTokenResultCallBack)
    val issueTime = time.milliseconds
    val maxLifeTime = issueTime + maxLifeTimeMsDefault
    val tokenId = createTokenResult.tokenId
    val password = DelegationTokenManager.createHmac(tokenId, secretKey)
    assertEquals(CreateTokenResult(owner, owner, issueTime, issueTime + renewTimeMsDefault,  maxLifeTime, tokenId, password, Errors.NONE), createTokenResult)

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
    val tokenManager = createDelegationTokenManager(config, tokenCache, time, zkClient)
    tokenManager.startup()

    tokenManager.createToken(owner, owner, renewer, -1 , createTokenResultCallBack)
    val issueTime = time.milliseconds
    val tokenId = createTokenResult.tokenId
    val password = DelegationTokenManager.createHmac(tokenId, secretKey)
    assertEquals(CreateTokenResult(owner, owner, issueTime, issueTime + renewTimeMsDefault,  issueTime + maxLifeTimeMsDefault, tokenId, password, Errors.NONE), createTokenResult)

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
  def testRemoveTokenHmac():Unit = {
    val config = KafkaConfig.fromProps(props)
    val tokenManager = createDelegationTokenManager(config, tokenCache, time, zkClient)
    tokenManager.startup()

    tokenManager.createToken(owner, owner, renewer, -1 , createTokenResultCallBack)
    val issueTime = time.milliseconds
    val tokenId = createTokenResult.tokenId
    val password = DelegationTokenManager.createHmac(tokenId, secretKey)
    assertEquals(CreateTokenResult(owner, owner, issueTime, issueTime + renewTimeMsDefault,  issueTime + maxLifeTimeMsDefault, tokenId, password, Errors.NONE), createTokenResult)

    // expire the token immediately
    tokenManager.expireToken(owner, ByteBuffer.wrap(password), -1, renewResponseCallback)

    val encodedHmac = Base64.getEncoder.encodeToString(password)
    // check respective hmac map entry is removed for the expired tokenId.
    val tokenInformation = tokenManager.tokenCache.tokenIdForHmac(encodedHmac)
    assertNull(tokenInformation)

    //check that the token is removed
    assert(tokenManager.getToken(tokenId).isEmpty)
  }

  @Test
  def testDescribeToken(): Unit = {

    val config = KafkaConfig.fromProps(props)

    val requester1 = SecurityUtils.parseKafkaPrincipal("User:requester1")

    val owner1 = SecurityUtils.parseKafkaPrincipal("User:owner1")
    val owner2 = SecurityUtils.parseKafkaPrincipal("User:owner2")
    val owner3 = SecurityUtils.parseKafkaPrincipal("User:owner3")
    val owner4 = SecurityUtils.parseKafkaPrincipal("User:owner4")
    val owner5 = SecurityUtils.parseKafkaPrincipal("User:owner5")

    val renewer1 = SecurityUtils.parseKafkaPrincipal("User:renewer1")
    val renewer2 = SecurityUtils.parseKafkaPrincipal("User:renewer2")
    val renewer3 = SecurityUtils.parseKafkaPrincipal("User:renewer3")
    val renewer4 = SecurityUtils.parseKafkaPrincipal("User:renewer4")

    val aclAuthorizer = new AclAuthorizer
    aclAuthorizer.configure(config.originals)

    var hostSession = new Session(owner1, InetAddress.getByName("192.168.1.1"))

    val tokenManager = createDelegationTokenManager(config, tokenCache, time, zkClient)
    tokenManager.startup()

    //create tokens
    tokenManager.createToken(owner1, owner1, List(renewer1, renewer2), 1 * 60 * 60 * 1000L, createTokenResultCallBack)

    tokenManager.createToken(owner2, owner2, List(renewer3), 1 * 60 * 60 * 1000L, createTokenResultCallBack)
    val tokenId2 = createTokenResult.tokenId

    tokenManager.createToken(owner3, owner3, List(renewer4), 2 * 60 * 60 * 1000L, createTokenResultCallBack)
    val tokenId3 = createTokenResult.tokenId

    tokenManager.createToken(owner4, owner4, List(owner1, renewer4), 2 * 60 * 60 * 1000L, createTokenResultCallBack)

    tokenManager.createToken(requester1, owner5, List(renewer1), 1 * 60 * 60 * 1000L, createTokenResultCallBack)

    assertEquals(5, tokenManager.getAllTokenInformation.size)

    //get tokens non-exiting owner
    var  tokens = getTokens(tokenManager, aclAuthorizer, hostSession, owner1, List(SecurityUtils.parseKafkaPrincipal("User:unknown")))
    assertEquals(0, tokens.size)

    //get all tokens for  empty owner list
    tokens = getTokens(tokenManager, aclAuthorizer, hostSession, owner1, List())
    assertEquals(0, tokens.size)

    //get all tokens for owner1
    tokens = getTokens(tokenManager, aclAuthorizer, hostSession, owner1, List(owner1))
    assertEquals(2, tokens.size)

    //get all tokens for owner1
    tokens = getTokens(tokenManager, aclAuthorizer, hostSession, owner1, null)
    assertEquals(2, tokens.size)

    //get all tokens for unknown owner
    tokens = getTokens(tokenManager, aclAuthorizer, hostSession, SecurityUtils.parseKafkaPrincipal("User:unknown"), null)
    assertEquals(0, tokens.size)

    //get all tokens for multiple owners (owner1, renewer4) and without permission for renewer4
    tokens = getTokens(tokenManager, aclAuthorizer, hostSession, owner1, List(owner1, renewer4))
    assertEquals(2, tokens.size)

    // get tokens for owner5 with requester1
    tokens = getTokens(tokenManager, aclAuthorizer, hostSession, requester1, List(owner5))
    assertEquals(1, tokens.size)

    def createAcl(aclBinding: AclBinding): Unit = {
      val result = aclAuthorizer.createAcls(null, List(aclBinding).asJava).get(0).toCompletableFuture.get
      result.exception.ifPresent { e => throw e }
    }

    //get all tokens for multiple owners (owner1, renewer4) and with permission
    createAcl(new AclBinding(new ResourcePattern(DELEGATION_TOKEN, tokenId3, LITERAL),
      new AccessControlEntry(owner1.toString, WildcardHost, DESCRIBE, ALLOW)))
    tokens = getTokens(tokenManager, aclAuthorizer, hostSession, owner1, List(owner1, renewer4))
    assertEquals(3, tokens.size)

    //get all tokens for renewer4 which is a renewer principal for some tokens
    tokens = getTokens(tokenManager, aclAuthorizer, hostSession,  renewer4, List(renewer4))
    assertEquals(2, tokens.size)

    //get all tokens for multiple owners (renewer2, renewer3) which are token renewers principals and without permissions for renewer3
    tokens = getTokens(tokenManager, aclAuthorizer, hostSession,  renewer2, List(renewer2, renewer3))
    assertEquals(1, tokens.size)

    //get all tokens for multiple owners (renewer2, renewer3) which are token renewers principals and with permissions
    hostSession = Session(renewer2, InetAddress.getByName("192.168.1.1"))
    createAcl(new AclBinding(new ResourcePattern(DELEGATION_TOKEN, tokenId2, LITERAL),
      new AccessControlEntry(renewer2.toString, WildcardHost, DESCRIBE, ALLOW)))
    tokens = getTokens(tokenManager, aclAuthorizer, hostSession,  renewer2, List(renewer2, renewer3))
    assertEquals(2, tokens.size)

    aclAuthorizer.close()
  }

  private def getTokens(tokenManager: DelegationTokenManager, aclAuthorizer: AclAuthorizer, hostSession: Session,
                        requestPrincipal: KafkaPrincipal, requestedOwners: List[KafkaPrincipal]): List[DelegationToken] = {

    if (requestedOwners != null && requestedOwners.isEmpty) {
      List()
    }
    else {
      def authorizeToken(tokenId: String): Boolean = {
        val requestContext = AuthorizerUtils.sessionToRequestContext(hostSession)
        val action = new Action(AclOperation.DESCRIBE,
          new ResourcePattern(DELEGATION_TOKEN, tokenId, LITERAL), 1, true, true)
        aclAuthorizer.authorize(requestContext, List(action).asJava).asScala.head == AuthorizationResult.ALLOWED
      }
      def authorizeRequester(owner: KafkaPrincipal): Boolean = {
        val requestContext = AuthorizerUtils.sessionToRequestContext(hostSession)
        val action = new Action(AclOperation.DESCRIBE_TOKENS,
          new ResourcePattern(USER, owner.toString, LITERAL), 1, true, true)
        aclAuthorizer.authorize(requestContext, List(action).asJava).asScala.head == AuthorizationResult.ALLOWED
      }
      def eligible(token: TokenInformation) = DelegationTokenManager
        .filterToken(requestPrincipal, Option(requestedOwners), token, authorizeToken, authorizeRequester)
      tokenManager.getTokens(eligible)
    }
  }

  @Test
  def testPeriodicTokenExpiry(): Unit = {
    val config = KafkaConfig.fromProps(props)
    val tokenManager = createDelegationTokenManager(config, tokenCache, time, zkClient)
    tokenManager.startup()

    //create tokens
    tokenManager.createToken(owner, owner, renewer, 1 * 60 * 60 * 1000L, createTokenResultCallBack)
    tokenManager.createToken(owner, owner, renewer, 1 * 60 * 60 * 1000L, createTokenResultCallBack)
    tokenManager.createToken(owner, owner, renewer, 2 * 60 * 60 * 1000L, createTokenResultCallBack)
    tokenManager.createToken(owner, owner, renewer, 2 * 60 * 60 * 1000L, createTokenResultCallBack)
    assert(tokenManager.getAllTokenInformation.size == 4 )

    time.sleep(2 * 60 * 60 * 1000L)
    tokenManager.expireTokens()
    assert(tokenManager.getAllTokenInformation.size == 2 )

  }

  private def createTokenResultCallBack(ret: CreateTokenResult): Unit = {
    createTokenResult = ret
  }

  private def renewResponseCallback(ret: Errors, timeStamp: Long): Unit = {
    error = ret
    expiryTimeStamp = timeStamp
  }

  private def createDelegationTokenManager(config: KafkaConfig, tokenCache: DelegationTokenCache,
                                           time: Time, zkClient: KafkaZkClient): DelegationTokenManager = {
    val tokenManager = new DelegationTokenManager(config, tokenCache, time, zkClient)
    tokenManagers += tokenManager
    tokenManager
  }
}
