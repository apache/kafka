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

package kafka.server

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Base64

import kafka.common.{NotificationHandler, ZkNodeChangeNotificationListener}
import kafka.utils.{CoreUtils, Json}
import kafka.zk.{DelegationTokenChangeNotificationSequenceZNode, DelegationTokenChangeNotificationZNode, DelegationTokensZNode}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.{Sanitizer, SecurityUtils, Time}

import scala.jdk.CollectionConverters._
import scala.collection.mutable

/*
 * Object used to encode and decode TokenInformation to Zk
 */
object DelegationTokenManagerZk {
  val OwnerKey ="owner"
  val TokenRequesterKey = "tokenRequester"
  val RenewersKey = "renewers"
  val IssueTimestampKey = "issueTimestamp"
  val MaxTimestampKey = "maxTimestamp"
  val ExpiryTimestampKey = "expiryTimestamp"
  val TokenIdKey = "tokenId"
  val VersionKey = "version"
  val CurrentVersion = 3

  def toJsonCompatibleMap(tokenInfo: TokenInformation):  Map[String, Any] = {
    val tokenInfoMap = mutable.Map[String, Any]()
    tokenInfoMap(VersionKey) = CurrentVersion
    tokenInfoMap(OwnerKey) = Sanitizer.sanitize(tokenInfo.ownerAsString)
    tokenInfoMap(TokenRequesterKey) = Sanitizer.sanitize(tokenInfo.tokenRequester.toString)
    tokenInfoMap(RenewersKey) = tokenInfo.renewersAsString.asScala.map(e => Sanitizer.sanitize(e)).asJava
    tokenInfoMap(IssueTimestampKey) = tokenInfo.issueTimestamp
    tokenInfoMap(MaxTimestampKey) = tokenInfo.maxTimestamp
    tokenInfoMap(ExpiryTimestampKey) = tokenInfo.expiryTimestamp
    tokenInfoMap(TokenIdKey) = tokenInfo.tokenId()
    tokenInfoMap.toMap
  }

  def fromBytes(bytes: Array[Byte]): Option[TokenInformation] = {
    if (bytes == null || bytes.isEmpty)
      return None

    Json.parseBytes(bytes) match {
      case Some(js) =>
        val mainJs = js.asJsonObject
        val version = mainJs(VersionKey).to[Int]
        require(version > 0 && version <= CurrentVersion)
        val owner = SecurityUtils.parseKafkaPrincipal(Sanitizer.desanitize(mainJs(OwnerKey).to[String]))
        var tokenRequester = owner
        if (version >= 3)
          tokenRequester = SecurityUtils.parseKafkaPrincipal(Sanitizer.desanitize(mainJs(TokenRequesterKey).to[String]))
        val renewerStr = mainJs(RenewersKey).to[Seq[String]]
        val renewers = renewerStr.map(Sanitizer.desanitize).map(SecurityUtils.parseKafkaPrincipal)
        val issueTimestamp = mainJs(IssueTimestampKey).to[Long]
        val expiryTimestamp = mainJs(ExpiryTimestampKey).to[Long]
        val maxTimestamp = mainJs(MaxTimestampKey).to[Long]
        val tokenId = mainJs(TokenIdKey).to[String]

        val tokenInfo = new TokenInformation(tokenId, owner, tokenRequester, renewers.asJava,
          issueTimestamp, maxTimestamp, expiryTimestamp)

        Some(tokenInfo)
      case None =>
        None
    }
  }
}

/*
 * Cache for Delegation Tokens when using Zk for metadata.
 * This includes other Zk specific handling of Delegation Tokens.
 */
class DelegationTokenManagerZk(config: KafkaConfig,
                               tokenCache: DelegationTokenCache,
                               time: Time,
                               val zkClient: KafkaZkClient) 
  extends DelegationTokenManager(config, tokenCache, time) {

  import DelegationTokenManager._

  private var tokenChangeListener: ZkNodeChangeNotificationListener = _

  override def startup(): Unit = {
    if (config.tokenAuthEnabled) {
      zkClient.createDelegationTokenPaths()
      loadCache()
      tokenChangeListener = new ZkNodeChangeNotificationListener(zkClient, DelegationTokenChangeNotificationZNode.path, DelegationTokenChangeNotificationSequenceZNode.SequenceNumberPrefix, TokenChangedNotificationHandler)
      tokenChangeListener.init()
    }
  }

  private def loadCache(): Unit = {
    lock.synchronized {
      val tokens = zkClient.getChildren(DelegationTokensZNode.path)
      info(s"Loading the token cache. Total token count: ${tokens.size}")
      for (tokenId <- tokens) {
        try {
          getTokenFromZk(tokenId) match {
            case Some(token) => updateCache(token)
            case None =>
          }
        } catch {
          case ex: Throwable => error(s"Error while getting Token for tokenId: $tokenId", ex)
        }
      }
    }
  }

  private def getTokenFromZk(tokenId: String): Option[DelegationToken] = {
    zkClient.getDelegationTokenInfo(tokenId) match {
      case Some(tokenInformation) => {
        val hmac = createHmac(tokenId, secretKey)
        Some(new DelegationToken(tokenInformation, hmac))
      }
      case None =>
        None
    }
  }
  
  override def shutdown(): Unit = {
    if (config.tokenAuthEnabled) {
      if (tokenChangeListener != null) tokenChangeListener.close()
    }
  }

  /**
   * @param token
   */
  override def updateToken(token: DelegationToken): Unit = {
    zkClient.setOrCreateDelegationToken(token)
    updateCache(token)
    zkClient.createTokenChangeNotification(token.tokenInfo.tokenId())
  }

  /**
   *
   * @param owner
   * @param renewers
   * @param maxLifeTimeMs
   * @param responseCallback
   */
  override def createToken(owner: KafkaPrincipal,
                  tokenRequester: KafkaPrincipal,
                  renewers: List[KafkaPrincipal],
                  maxLifeTimeMs: Long,
                  responseCallback: CreateResponseCallback): Unit = {

    if (!config.tokenAuthEnabled) {
      responseCallback(CreateTokenResult(owner, tokenRequester, -1, -1, -1, "", Array[Byte](), Errors.DELEGATION_TOKEN_AUTH_DISABLED))
    } else {
      lock.synchronized {
        val tokenId = CoreUtils.generateUuidAsBase64()

        val issueTimeStamp = time.milliseconds
        val maxLifeTime = if (maxLifeTimeMs <= 0) tokenMaxLifetime else Math.min(maxLifeTimeMs, tokenMaxLifetime)
        val maxLifeTimeStamp = issueTimeStamp + maxLifeTime
        val expiryTimeStamp = Math.min(maxLifeTimeStamp, issueTimeStamp + defaultTokenRenewTime)

        val tokenInfo = new TokenInformation(tokenId, owner, tokenRequester, renewers.asJava, issueTimeStamp, maxLifeTimeStamp, expiryTimeStamp)

        val hmac = createHmac(tokenId, secretKey)
        val token = new DelegationToken(tokenInfo, hmac)
        updateToken(token)
        info(s"Created a delegation token: $tokenId for owner: $owner")
        responseCallback(CreateTokenResult(owner, tokenRequester, issueTimeStamp, expiryTimeStamp, maxLifeTimeStamp, tokenId, hmac, Errors.NONE))
      }
    }
  }

  /**
   *
   * @param hmac
   * @return
   */
  private def getToken(hmac: ByteBuffer): Option[DelegationToken] = {
    try {
      val byteArray = new Array[Byte](hmac.remaining)
      hmac.get(byteArray)
      val base64Pwd = Base64.getEncoder.encodeToString(byteArray)
      val tokenInfo = tokenCache.tokenForHmac(base64Pwd)
      if (tokenInfo == null) None else Some(new DelegationToken(tokenInfo, byteArray))
    } catch {
      case e: Exception =>
        error("Exception while getting token for hmac", e)
        None
    }
  }

  /**
   *
   * @param principal
   * @param tokenInfo
   * @return
   */
  private def allowedToRenew(principal: KafkaPrincipal, tokenInfo: TokenInformation): Boolean = {
    if (principal.equals(tokenInfo.owner) || tokenInfo.renewers.asScala.toList.contains(principal)) true else false
  }

  /**
   *
   * @param principal
   * @param hmac
   * @param renewLifeTimeMs
   * @param renewResponseCallback
   */
  override def renewToken(principal: KafkaPrincipal,
                 hmac: ByteBuffer,
                 renewLifeTimeMs: Long,
                 renewCallback: RenewResponseCallback): Unit = {

    if (!config.tokenAuthEnabled) {
      renewCallback(Errors.DELEGATION_TOKEN_AUTH_DISABLED, -1)
    } else {
      lock.synchronized  {
        getToken(hmac) match {
          case Some(token) => {
            val now = time.milliseconds
            val tokenInfo =  token.tokenInfo

            if (!allowedToRenew(principal, tokenInfo)) {
              renewCallback(Errors.DELEGATION_TOKEN_OWNER_MISMATCH, -1)
            } else if (tokenInfo.maxTimestamp < now || tokenInfo.expiryTimestamp < now) {
              renewCallback(Errors.DELEGATION_TOKEN_EXPIRED, -1)
            } else {
              val renewLifeTime = if (renewLifeTimeMs < 0) defaultTokenRenewTime else renewLifeTimeMs
              val renewTimeStamp = now + renewLifeTime
              val expiryTimeStamp = Math.min(tokenInfo.maxTimestamp, renewTimeStamp)
              tokenInfo.setExpiryTimestamp(expiryTimeStamp)

              updateToken(token)
              info(s"Delegation token renewed for token: ${tokenInfo.tokenId} for owner: ${tokenInfo.owner}")
              renewCallback(Errors.NONE, expiryTimeStamp)
            }
          }
          case None => renewCallback(Errors.DELEGATION_TOKEN_NOT_FOUND, -1)
        }
      }
    }
  }

  /**
   *
   * @param principal
   * @param hmac
   * @param expireLifeTimeMs
   * @param expireResponseCallback
   */
  override def expireToken(principal: KafkaPrincipal,
                  hmac: ByteBuffer,
                  expireLifeTimeMs: Long,
                  expireResponseCallback: ExpireResponseCallback): Unit = {

    if (!config.tokenAuthEnabled) {
      expireResponseCallback(Errors.DELEGATION_TOKEN_AUTH_DISABLED, -1)
    } else {
      lock.synchronized  {
        getToken(hmac) match {
          case Some(token) =>  {
            val tokenInfo =  token.tokenInfo
            val now = time.milliseconds

            if (!allowedToRenew(principal, tokenInfo)) {
              expireResponseCallback(Errors.DELEGATION_TOKEN_OWNER_MISMATCH, -1)
            } else if (expireLifeTimeMs < 0) { //expire immediately
              removeToken(tokenInfo.tokenId)
              info(s"Token expired for token: ${tokenInfo.tokenId} for owner: ${tokenInfo.owner}")
              expireResponseCallback(Errors.NONE, now)
            } else if (tokenInfo.maxTimestamp < now || tokenInfo.expiryTimestamp < now) {
              expireResponseCallback(Errors.DELEGATION_TOKEN_EXPIRED, -1)
            } else {
              //set expiry time stamp
              val expiryTimeStamp = Math.min(tokenInfo.maxTimestamp, now + expireLifeTimeMs)
              tokenInfo.setExpiryTimestamp(expiryTimeStamp)

              updateToken(token)
              info(s"Updated expiry time for token: ${tokenInfo.tokenId} for owner: ${tokenInfo.owner}")
              expireResponseCallback(Errors.NONE, expiryTimeStamp)
            }
          }
          case None => expireResponseCallback(Errors.DELEGATION_TOKEN_NOT_FOUND, -1)
        }
      }
    }
  }

  /**
   *
   * @param tokenId
   */
  override def removeToken(tokenId: String): Unit = {
    zkClient.deleteDelegationToken(tokenId)
    removeCache(tokenId)
    zkClient.createTokenChangeNotification(tokenId)
  }

  /**
   *
   * @return
   */
  override def expireTokens(): Unit = {
    lock.synchronized {
      for (tokenInfo <- getAllTokenInformation) {
        val now = time.milliseconds
        if (tokenInfo.maxTimestamp < now || tokenInfo.expiryTimestamp < now) {
          info(s"Delegation token expired for token: ${tokenInfo.tokenId} for owner: ${tokenInfo.owner}")
          removeToken(tokenInfo.tokenId)
        }
      }
    }
  }

  object TokenChangedNotificationHandler extends NotificationHandler {
    override def processNotification(tokenIdBytes: Array[Byte]): Unit = {
      lock.synchronized {
        val tokenId = new String(tokenIdBytes, StandardCharsets.UTF_8)
        info(s"Processing Token Notification for tokenId: $tokenId")
        getTokenFromZk(tokenId) match {
          case Some(token) => updateCache(token)
          case None => removeCache(tokenId)
        }
      }
    }
  }
}

