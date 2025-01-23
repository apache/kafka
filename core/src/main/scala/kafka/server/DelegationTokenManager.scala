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

import java.nio.charset.StandardCharsets
import java.security.InvalidKeyException
import javax.crypto.spec.SecretKeySpec
import javax.crypto.{Mac, SecretKey}
import kafka.utils.Logging
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.scram.internals.{ScramFormatter, ScramMechanism}
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.Time

import java.util
import java.util.stream.Collectors
import scala.jdk.CollectionConverters._
import scala.collection.mutable

object DelegationTokenManager {
  private val DefaultHmacAlgorithm = "HmacSHA512"
  val ErrorTimestamp = -1

  /**
   * Convert the byte[] to a secret key
   * @param keybytes the byte[] to create the secret key from
   * @return the secret key
   */
  private def createSecretKey(keybytes: Array[Byte]) : SecretKey = {
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
      case ike: InvalidKeyException => throw new IllegalArgumentException("Invalid key to HMAC computation", ike)
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

  import DelegationTokenManager._

  val secretKey: SecretKey = {
    val keyBytes =  if (config.tokenAuthEnabled) config.delegationTokenSecretKey.value.getBytes(StandardCharsets.UTF_8) else null
    if (keyBytes == null || keyBytes.isEmpty) null
    else
      createSecretKey(keyBytes)
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
    val hmacString = token.hmacAsBase64String
    val scramCredentialMap = prepareScramCredentials(hmacString)
    tokenCache.updateCache(token, scramCredentialMap.asJava)
  }

  def getDelegationToken(tokenInfo: TokenInformation): DelegationToken = {
    val hmac = createHmac(tokenInfo.tokenId, secretKey)
    new DelegationToken(tokenInfo, hmac)
  }

  /**
   *
   * @param tokenId
   */
  def removeToken(tokenId: String): Unit = {
    tokenCache.removeCache(tokenId)
  }

  def getTokens(filterToken: TokenInformation => Boolean): util.List[DelegationToken] = {
    tokenCache.tokens.stream().filter(token => filterToken(token)).map(getDelegationToken).collect(Collectors.toList[DelegationToken])
  }
}
