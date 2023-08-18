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
import kafka.utils.Logging
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.scram.internals.{ScramFormatter, ScramMechanism}
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters._
import scala.collection.mutable

object DelegationTokenManager {
  val DefaultHmacAlgorithm = "HmacSHA512"
  val CurrentVersion = 3
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

  def filterToken(requesterPrincipal: KafkaPrincipal, owners : Option[List[KafkaPrincipal]], token: TokenInformation,
                  authorizeToken: String => Boolean, authorizeRequester: KafkaPrincipal  => Boolean) : Boolean = {

    val allow =
    //exclude tokens which are not requested
      if (owners.isDefined && !owners.get.exists(owner => token.ownerOrRenewer(owner))) {
        false
        //Owners and the renewers can describe their own tokens
      } else if (token.ownerOrRenewer(requesterPrincipal)) {
        true
        // Check permission for non-owned tokens
      } else if (authorizeToken(token.tokenId) || authorizeRequester(token.owner)) {
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
                             val time: Time) extends Logging {
  this.logIdent = s"[Token Manager on Node ${config.brokerId}]: "

  protected val lock = new Object()

  import DelegationTokenManager._

  type CreateResponseCallback = CreateTokenResult => Unit
  type RenewResponseCallback = (Errors, Long) => Unit
  type ExpireResponseCallback = (Errors, Long) => Unit

  val secretKey = {
    val keyBytes =  if (config.tokenAuthEnabled) config.delegationTokenSecretKey.value.getBytes(StandardCharsets.UTF_8) else null
    if (keyBytes == null || keyBytes.isEmpty) null
    else
      createSecretKey(keyBytes)
  }

  val tokenMaxLifetime: Long = config.delegationTokenMaxLifeMs
  val defaultTokenRenewTime: Long = config.delegationTokenExpiryTimeMs

  def startup(): Unit = {
      // Nothing to do. Overridden for Zk case
  }

  def shutdown(): Unit = {
      // Nothing to do. Overridden for Zk case
  }

  /**
   *
   * @param token
   */
  protected def updateCache(token: DelegationToken): Unit = {
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
   * @param token
   */
  def updateToken(token: DelegationToken): Unit = {
    updateCache(token)
  }

  /**
   *
   * @param owner
   * @param renewers
   * @param maxLifeTimeMs
   * @param responseCallback
   */
  def createToken(owner: KafkaPrincipal,
                  tokenRequester: KafkaPrincipal,
                  renewers: List[KafkaPrincipal],
                  maxLifeTimeMs: Long,
                  responseCallback: CreateResponseCallback): Unit = {
    // Must be forwarded to KRaft Controller or handled in DelegationTokenManagerZk
    throw new IllegalStateException("API createToken was not forwarded to a handler.")
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
                 renewCallback: RenewResponseCallback): Unit = {
    // Must be forwarded to KRaft Controller or handled in DelegationTokenManagerZk
    throw new IllegalStateException("API renewToken was not forwarded to a handler.")
  }

  def getDelegationToken(tokenInfo: TokenInformation): DelegationToken = {
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
                  expireResponseCallback: ExpireResponseCallback): Unit = {
    // Must be forwarded to KRaft Controller or handled in DelegationTokenManagerZk
    throw new IllegalStateException("API expireToken was not forwarded to a handler.")
  }

  /**
   *
   * @param tokenId
   */
  def removeToken(tokenId: String): Unit = {
    removeCache(tokenId)
  }

  /**
   *
   * @param tokenId
   */
  protected def removeCache(tokenId: String): Unit = {
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
          info(s"Delegation token expired for token: ${tokenInfo.tokenId} for owner: ${tokenInfo.owner}")
          removeToken(tokenInfo.tokenId)
        }
      }
    }
  }

  def getAllTokenInformation: List[TokenInformation] = tokenCache.tokens.asScala.toList

  def getTokens(filterToken: TokenInformation => Boolean): List[DelegationToken] = {
    getAllTokenInformation.filter(filterToken).map(token => getDelegationToken(token))
  }

}

case class CreateTokenResult(owner: KafkaPrincipal,
                             tokenRequester: KafkaPrincipal,
                             issueTimestamp: Long,
                             expiryTimestamp: Long,
                             maxTimestamp: Long,
                             tokenId: String,
                             hmac: Array[Byte],
                             error: Errors) {

  override def equals(other: Any): Boolean = {
    other match {
      case that: CreateTokenResult =>
        error.equals(that.error) &&
          owner.equals(that.owner) &&
          tokenRequester.equals(that.tokenRequester) &&
          tokenId.equals(that.tokenId) &&
          issueTimestamp.equals(that.issueTimestamp) &&
          expiryTimestamp.equals(that.expiryTimestamp) &&
          maxTimestamp.equals(that.maxTimestamp) &&
          (hmac sameElements that.hmac)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val fields = Seq(owner, tokenRequester, issueTimestamp, expiryTimestamp, maxTimestamp, tokenId, hmac, error)
    fields.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
