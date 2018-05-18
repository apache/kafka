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
import java.security.InvalidKeyException
import javax.crypto.spec.SecretKeySpec
import javax.crypto.{Mac, SecretKey}

import kafka.common.{NotificationHandler, ZkNodeChangeNotificationListener}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{CoreUtils, Json, Logging}
import kafka.zk.{DelegationTokenChangeNotificationSequenceZNode, DelegationTokenChangeNotificationZNode, DelegationTokensZNode, KafkaZkClient}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.scram.internal.{ScramFormatter, ScramMechanism}
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.token.delegation.internal.DelegationTokenCache
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.{Base64, Sanitizer, SecurityUtils, Time}

import scala.collection.JavaConverters._
import scala.collection.mutable

object DelegationTokenManager {
  val DefaultHmacAlgorithm = "HmacSHA512"
  val OwnerKey ="owner"
  val RenewersKey = "renewers"
  val IssueTimestampKey = "issueTimestamp"
  val MaxTimestampKey = "maxTimestamp"
  val ExpiryTimestampKey = "expiryTimestamp"
  val TokenIdKey = "tokenId"
  val VersionKey = "version"
  val CurrentVersion = 1
  val ErrorTimestamp = -1

  /**
   *
   * @param tokenId
   * @param secretKey
   * @return
   */
  def createHmac(tokenId: String, secretKey: String) : Array[Byte] = {
    createHmac(tokenId, createSecretKey(secretKey.getBytes(StandardCharsets.UTF_8)))
  }

  /**
   * Convert the byte[] to a secret key
   * @param keybytes the byte[] to create the secret key from
   * @return the secret key
   */
  def createSecretKey(keybytes: Array[Byte]) : SecretKey = {
    new SecretKeySpec(keybytes, DefaultHmacAlgorithm)
  }

  /**
   *
   *
   * @param tokenId
   * @param secretKey
   * @return
   */
  def createBase64HMAC(tokenId: String, secretKey: SecretKey) : String = {
    val hmac = createHmac(tokenId, secretKey)
    Base64.encoder.encodeToString(hmac)
  }

  /**
   * Compute HMAC of the identifier using the secret key
   * @param tokenId the bytes of the identifier
   * @param secretKey  the secret key
   * @return  String of the generated hmac
   */
  def createHmac(tokenId: String, secretKey: SecretKey) : Array[Byte] = {
    val mac =  Mac.getInstance(DefaultHmacAlgorithm)
    try
      mac.init(secretKey)
    catch {
      case ike: InvalidKeyException => throw new IllegalArgumentException("Invalid key to HMAC computation", ike);
    }
    mac.doFinal(tokenId.getBytes(StandardCharsets.UTF_8))
  }

  def toJsonCompatibleMap(token: DelegationToken):  Map[String, Any] = {
    val tokenInfo = token.tokenInfo
    val tokenInfoMap = mutable.Map[String, Any]()
    tokenInfoMap(VersionKey) = CurrentVersion
    tokenInfoMap(OwnerKey) = Sanitizer.sanitize(tokenInfo.ownerAsString)
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
        require(mainJs(VersionKey).to[Int] == CurrentVersion)
        val owner = SecurityUtils.parseKafkaPrincipal(Sanitizer.desanitize(mainJs(OwnerKey).to[String]))
        val renewerStr = mainJs(RenewersKey).to[Seq[String]]
        val renewers = renewerStr.map(Sanitizer.desanitize(_)).map(SecurityUtils.parseKafkaPrincipal(_))
        val issueTimestamp = mainJs(IssueTimestampKey).to[Long]
        val expiryTimestamp = mainJs(ExpiryTimestampKey).to[Long]
        val maxTimestamp = mainJs(MaxTimestampKey).to[Long]
        val tokenId = mainJs(TokenIdKey).to[String]

        val tokenInfo = new TokenInformation(tokenId, owner, renewers.asJava,
          issueTimestamp, maxTimestamp, expiryTimestamp)

        Some(tokenInfo)
      case None =>
        None
    }
  }

  def filterToken(requestedPrincipal: KafkaPrincipal, owners : Option[List[KafkaPrincipal]], token: TokenInformation, authorizeToken: String => Boolean) : Boolean = {

    val allow =
    //exclude tokens which are not requested
      if (!owners.isEmpty && !owners.get.exists(owner => token.ownerOrRenewer(owner))) {
        false
        //Owners and the renewers can describe their own tokens
      } else if (token.ownerOrRenewer(requestedPrincipal)) {
        true
        // Check permission for non-owned tokens
      } else if ((authorizeToken(token.tokenId))) {
        true
      }
      else {
        false
      }

    allow
  }
}

class DelegationTokenManager(val config: KafkaConfig,
                             val tokenCache: DelegationTokenCache,
                             val time: Time,
                             val zkClient: KafkaZkClient) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Token Manager on Broker " + config.brokerId + "]: "

  import DelegationTokenManager._

  type CreateResponseCallback = CreateTokenResult => Unit
  type RenewResponseCallback = (Errors, Long) => Unit
  type ExpireResponseCallback = (Errors, Long) => Unit
  type DescribeResponseCallback = (Errors, List[DelegationToken]) => Unit

  val secretKey = {
    val keyBytes =  if (config.tokenAuthEnabled) config.delegationTokenMasterKey.value.getBytes(StandardCharsets.UTF_8) else null
    if (keyBytes == null || keyBytes.length == 0) null
    else
      createSecretKey(keyBytes)
  }

  val tokenMaxLifetime: Long = config.delegationTokenMaxLifeMs
  val defaultTokenRenewTime: Long = config.delegationTokenExpiryTimeMs
  val tokenRemoverScanInterval: Long = config.delegationTokenExpiryCheckIntervalMs
  private val lock = new Object()
  private var tokenChangeListener: ZkNodeChangeNotificationListener = null

  def startup() = {
    if (config.tokenAuthEnabled) {
      zkClient.createDelegationTokenPaths
      loadCache
      tokenChangeListener = new ZkNodeChangeNotificationListener(zkClient, DelegationTokenChangeNotificationZNode.path, DelegationTokenChangeNotificationSequenceZNode.SequenceNumberPrefix, TokenChangedNotificationHandler)
      tokenChangeListener.init
    }
  }

  def shutdown() = {
    if (config.tokenAuthEnabled) {
      if (tokenChangeListener != null) tokenChangeListener.close()
    }
  }

  private def loadCache() {
    lock.synchronized {
      val tokens = zkClient.getChildren(DelegationTokensZNode.path)
      info(s"Loading the token cache. Total token count : " + tokens.size)
      for (tokenId <- tokens) {
        try {
          getTokenFromZk(tokenId) match {
            case Some(token) => updateCache(token)
            case None =>
          }
        } catch {
          case ex: Throwable => error(s"Error while getting Token for tokenId :$tokenId", ex)
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

  /**
   *
   * @param token
   */
  private def updateCache(token: DelegationToken): Unit = {
    val hmacString = token.hmacAsBase64String
    val scramCredentialMap =  prepareScramCredentials(hmacString)
    tokenCache.updateCache(token, scramCredentialMap.asJava)
  }
  /**
   * @param hmacString
   */
  private def prepareScramCredentials(hmacString: String) : Map[String, ScramCredential] = {
    val scramCredentialMap = mutable.Map[String, ScramCredential]()

    def scramCredential(mechanism: ScramMechanism): ScramCredential = {
      new ScramFormatter(mechanism).generateCredential(hmacString, mechanism.minIterations)
    }

    for (mechanism <- ScramMechanism.values)
      scramCredentialMap(mechanism.mechanismName) = scramCredential(mechanism)

    scramCredentialMap.toMap
  }

  /**
   *
   * @param owner
   * @param renewers
   * @param maxLifeTimeMs
   * @param responseCallback
   */
  def createToken(owner: KafkaPrincipal,
                  renewers: List[KafkaPrincipal],
                  maxLifeTimeMs: Long,
                  responseCallback: CreateResponseCallback) {

    if (!config.tokenAuthEnabled) {
      responseCallback(CreateTokenResult(-1, -1, -1, "", Array[Byte](), Errors.DELEGATION_TOKEN_AUTH_DISABLED))
    } else {
      lock.synchronized {
        val tokenId = CoreUtils.generateUuidAsBase64

        val issueTimeStamp = time.milliseconds
        val maxLifeTime = if (maxLifeTimeMs <= 0) tokenMaxLifetime else Math.min(maxLifeTimeMs, tokenMaxLifetime)
        val maxLifeTimeStamp = issueTimeStamp + maxLifeTime
        val expiryTimeStamp = Math.min(maxLifeTimeStamp, issueTimeStamp + defaultTokenRenewTime)

        val tokenInfo = new TokenInformation(tokenId, owner, renewers.asJava, issueTimeStamp, maxLifeTimeStamp, expiryTimeStamp)

        val hmac = createHmac(tokenId, secretKey)
        val token = new DelegationToken(tokenInfo, hmac)
        updateToken(token)
        info(s"Created a delegation token : $tokenId for owner : $owner")
        responseCallback(CreateTokenResult(issueTimeStamp, expiryTimeStamp, maxLifeTimeStamp, tokenId, hmac, Errors.NONE))
      }
    }
  }

  /**
   *
   * @param principal
   * @param hmac
   * @param renewLifeTimeMs
   * @param renewCallback
   */
  def renewToken(principal: KafkaPrincipal,
                 hmac: ByteBuffer,
                 renewLifeTimeMs: Long,
                 renewCallback: RenewResponseCallback) {

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
              info(s"Delegation token renewed for token : " + tokenInfo.tokenId + " for owner :" + tokenInfo.owner)
              renewCallback(Errors.NONE, expiryTimeStamp)
            }
          }
          case None => renewCallback(Errors.DELEGATION_TOKEN_NOT_FOUND, -1)
        }
      }
    }
  }

  /**
   * @param token
   */
  private def updateToken(token: DelegationToken): Unit = {
    zkClient.setOrCreateDelegationToken(token)
    updateCache(token)
    zkClient.createTokenChangeNotification(token.tokenInfo.tokenId())
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
      val base64Pwd = Base64.encoder.encodeToString(byteArray)
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
   * @param tokenId
   * @return
   */
  def getToken(tokenId: String): Option[DelegationToken] = {
    val tokenInfo = tokenCache.token(tokenId)
    if (tokenInfo != null) Some(getToken(tokenInfo)) else None
  }

  /**
   *
   * @param tokenInfo
   * @return
   */
  private def getToken(tokenInfo: TokenInformation): DelegationToken = {
    val hmac = createHmac(tokenInfo.tokenId, secretKey)
    new DelegationToken(tokenInfo, hmac)
  }

  /**
   *
   * @param principal
   * @param hmac
   * @param expireLifeTimeMs
   * @param expireResponseCallback
   */
  def expireToken(principal: KafkaPrincipal,
                  hmac: ByteBuffer,
                  expireLifeTimeMs: Long,
                  expireResponseCallback: ExpireResponseCallback) {

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
            } else if (tokenInfo.maxTimestamp < now || tokenInfo.expiryTimestamp < now) {
              expireResponseCallback(Errors.DELEGATION_TOKEN_EXPIRED, -1)
            } else if (expireLifeTimeMs < 0) { //expire immediately
              removeToken(tokenInfo.tokenId)
              info(s"Token expired for token : " + tokenInfo.tokenId + " for owner :" + tokenInfo.owner)
              expireResponseCallback(Errors.NONE, now)
            } else {
              //set expiry time stamp
              val expiryTimeStamp = Math.min(tokenInfo.maxTimestamp, now + expireLifeTimeMs)
              tokenInfo.setExpiryTimestamp(expiryTimeStamp)

              updateToken(token)
              info(s"Updated expiry time for token : " + tokenInfo.tokenId + " for owner :" + tokenInfo.owner)
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
  private def removeToken(tokenId: String): Unit = {
    zkClient.deleteDelegationToken(tokenId)
    removeCache(tokenId)
    zkClient.createTokenChangeNotification(tokenId)
  }

  /**
   *
   * @param tokenId
   */
  private def removeCache(tokenId: String): Unit = {
    tokenCache.removeCache(tokenId)
  }

  /**
   *
   * @return
   */
  def expireTokens(): Unit = {
    lock.synchronized {
      for (tokenInfo <- getAllTokenInformation) {
        val now = time.milliseconds
        if (tokenInfo.maxTimestamp < now || tokenInfo.expiryTimestamp < now) {
          info(s"Delegation token expired for token : " + tokenInfo.tokenId + " for owner :" + tokenInfo.owner)
          removeToken(tokenInfo.tokenId)
        }
      }
    }
  }

  /**
   *
   * @return
   */
  def getAllTokenInformation(): List[TokenInformation] = {
    tokenCache.tokens.asScala.toList
  }

  def getTokens(filterToken: TokenInformation => Boolean): List[DelegationToken] = {
    getAllTokenInformation().filter(filterToken).map(token => getToken(token))
  }

  object TokenChangedNotificationHandler extends NotificationHandler {
    override def processNotification(tokenIdBytes: Array[Byte]) {
      lock.synchronized {
        val tokenId = new String(tokenIdBytes, StandardCharsets.UTF_8)
        info(s"Processing Token Notification for tokenId : $tokenId")
        getTokenFromZk(tokenId) match {
          case Some(token) => updateCache(token)
          case None => removeCache(tokenId)
        }
      }
    }
  }

}

case class CreateTokenResult(issueTimestamp: Long,
                             expiryTimestamp: Long,
                             maxTimestamp: Long,
                             tokenId: String,
                             hmac: Array[Byte],
                             error: Errors) {

  override def equals(other: Any): Boolean = {
    other match {
      case that: CreateTokenResult =>
        error.equals(that.error) &&
          tokenId.equals(that.tokenId) &&
          issueTimestamp.equals(that.issueTimestamp) &&
          expiryTimestamp.equals(that.expiryTimestamp) &&
          maxTimestamp.equals(that.maxTimestamp) &&
          (hmac sameElements that.hmac)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val fields = Seq(issueTimestamp, expiryTimestamp, maxTimestamp, tokenId, hmac, error)
    fields.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}