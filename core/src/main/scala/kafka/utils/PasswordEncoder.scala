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
package kafka.utils

import java.nio.charset.StandardCharsets
import java.security.{AlgorithmParameters, NoSuchAlgorithmException, SecureRandom}
import java.security.spec.AlgorithmParameterSpec
import java.util.Base64

import javax.crypto.{Cipher, SecretKeyFactory}
import javax.crypto.spec._
import kafka.utils.PasswordEncoder._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password

import scala.collection.Map

object PasswordEncoder {
  val KeyFactoryAlgorithmProp = "keyFactoryAlgorithm"
  val CipherAlgorithmProp = "cipherAlgorithm"
  val InitializationVectorProp = "initializationVector"
  val KeyLengthProp = "keyLength"
  val SaltProp = "salt"
  val IterationsProp = "iterations"
  val EncryptedPasswordProp = "encryptedPassword"
  val PasswordLengthProp = "passwordLength"

  def encrypting(secret: Password,
                 keyFactoryAlgorithm: Option[String],
                 cipherAlgorithm: String,
                 keyLength: Int,
                 iterations: Int): EncryptingPasswordEncoder = {
    new EncryptingPasswordEncoder(secret, keyFactoryAlgorithm, cipherAlgorithm, keyLength, iterations)
  }

  def noop(): NoOpPasswordEncoder = {
    new NoOpPasswordEncoder()
  }
}

trait PasswordEncoder {
  def encode(password: Password): String
  def decode(encodedPassword: String): Password

  private[utils] def base64Decode(encoded: String): Array[Byte] = Base64.getDecoder.decode(encoded)
}

/**
 * A password encoder that does not modify the given password. This is used in KRaft mode only.
 */
class NoOpPasswordEncoder extends PasswordEncoder {
  override def encode(password: Password): String = password.value()
  override def decode(encodedPassword: String): Password = new Password(encodedPassword)
}

/**
  * Password encoder and decoder implementation. Encoded passwords are persisted as a CSV map
  * containing the encoded password in base64 and along with the properties used for encryption.
  *
  * @param secret The secret used for encoding and decoding
  * @param keyFactoryAlgorithm  Key factory algorithm if configured. By default, PBKDF2WithHmacSHA512 is
  *                             used if available, PBKDF2WithHmacSHA1 otherwise.
  * @param cipherAlgorithm Cipher algorithm used for encoding.
  * @param keyLength Key length used for encoding. This should be valid for the specified algorithms.
  * @param iterations Iteration count used for encoding.
  *
  * The provided `keyFactoryAlgorithm`, `cipherAlgorithm`, `keyLength` and `iterations` are used for encoding passwords.
  * The values used for encoding are stored along with the encoded password and the stored values are used for decoding.
  *
  */
class EncryptingPasswordEncoder(
  secret: Password,
  keyFactoryAlgorithm: Option[String],
  cipherAlgorithm: String,
  keyLength: Int,
  iterations: Int
) extends PasswordEncoder with Logging {

  private val secureRandom = new SecureRandom
  private val cipherParamsEncoder = cipherParamsInstance(cipherAlgorithm)

  override def encode(password: Password): String = {
    val salt = new Array[Byte](256)
    secureRandom.nextBytes(salt)
    val cipher = Cipher.getInstance(cipherAlgorithm)
    val keyFactory = secretKeyFactory(keyFactoryAlgorithm)
    val keySpec = secretKeySpec(keyFactory, cipherAlgorithm, keyLength, salt, iterations)
    cipher.init(Cipher.ENCRYPT_MODE, keySpec)
    val encryptedPassword = cipher.doFinal(password.value.getBytes(StandardCharsets.UTF_8))
    val encryptedMap = Map(
      KeyFactoryAlgorithmProp -> keyFactory.getAlgorithm,
      CipherAlgorithmProp -> cipherAlgorithm,
      KeyLengthProp -> keyLength,
      SaltProp -> base64Encode(salt),
      IterationsProp -> iterations.toString,
      EncryptedPasswordProp -> base64Encode(encryptedPassword),
      PasswordLengthProp -> password.value.length
    ) ++ cipherParamsEncoder.toMap(cipher.getParameters)
    encryptedMap.map { case (k, v) => s"$k:$v" }.mkString(",")
  }

  override def decode(encodedPassword: String): Password = {
    val params = CoreUtils.parseCsvMap(encodedPassword)
    val keyFactoryAlg = params(KeyFactoryAlgorithmProp)
    val cipherAlg = params(CipherAlgorithmProp)
    val keyLength = params(KeyLengthProp).toInt
    val salt = base64Decode(params(SaltProp))
    val iterations = params(IterationsProp).toInt
    val encryptedPassword = base64Decode(params(EncryptedPasswordProp))
    val passwordLengthProp = params(PasswordLengthProp).toInt
    val cipher = Cipher.getInstance(cipherAlg)
    val keyFactory = secretKeyFactory(Some(keyFactoryAlg))
    val keySpec = secretKeySpec(keyFactory, cipherAlg, keyLength, salt, iterations)
    cipher.init(Cipher.DECRYPT_MODE, keySpec, cipherParamsEncoder.toParameterSpec(params))
    val password = try {
      val decrypted = cipher.doFinal(encryptedPassword)
      new String(decrypted, StandardCharsets.UTF_8)
    } catch {
      case e: Exception => throw new ConfigException("Password could not be decoded", e)
    }
    if (password.length != passwordLengthProp) // Sanity check
      throw new ConfigException("Password could not be decoded, sanity check of length failed")
    new Password(password)
  }

  private def secretKeyFactory(keyFactoryAlg: Option[String]): SecretKeyFactory = {
    keyFactoryAlg match {
      case Some(algorithm) => SecretKeyFactory.getInstance(algorithm)
      case None =>
        try {
          SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512")
        } catch {
          case _: NoSuchAlgorithmException => SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
        }
    }
  }

  private def secretKeySpec(keyFactory: SecretKeyFactory,
                            cipherAlg: String,
                            keyLength: Int,
                            salt: Array[Byte], iterations: Int): SecretKeySpec = {
    val keySpec = new PBEKeySpec(secret.value.toCharArray, salt, iterations, keyLength)
    val algorithm = if (cipherAlg.indexOf('/') > 0) cipherAlg.substring(0, cipherAlg.indexOf('/')) else cipherAlg
    new SecretKeySpec(keyFactory.generateSecret(keySpec).getEncoded, algorithm)
  }

  private def base64Encode(bytes: Array[Byte]): String = Base64.getEncoder.encodeToString(bytes)

  private def cipherParamsInstance(cipherAlgorithm: String): CipherParamsEncoder = {
    val aesPattern = "AES/(.*)/.*".r
    cipherAlgorithm match {
      case aesPattern("GCM") => new GcmParamsEncoder
      case _ => new IvParamsEncoder
    }
  }

  private trait CipherParamsEncoder {
    def toMap(cipher: AlgorithmParameters): Map[String, String]
    def toParameterSpec(paramMap: Map[String, String]): AlgorithmParameterSpec
  }

  private class IvParamsEncoder extends CipherParamsEncoder {
    def toMap(cipherParams: AlgorithmParameters): Map[String, String] = {
      if (cipherParams != null) {
        val ivSpec = cipherParams.getParameterSpec(classOf[IvParameterSpec])
        Map(InitializationVectorProp -> base64Encode(ivSpec.getIV))
      } else
        throw new IllegalStateException("Could not determine initialization vector for cipher")
    }
    def toParameterSpec(paramMap: Map[String, String]): AlgorithmParameterSpec = {
      new IvParameterSpec(base64Decode(paramMap(InitializationVectorProp)))
    }
  }

  private class GcmParamsEncoder extends CipherParamsEncoder {
    def toMap(cipherParams: AlgorithmParameters): Map[String, String] = {
      if (cipherParams != null) {
        val spec = cipherParams.getParameterSpec(classOf[GCMParameterSpec])
        Map(InitializationVectorProp -> base64Encode(spec.getIV),
            "authenticationTagLength" -> spec.getTLen.toString)
      } else
        throw new IllegalStateException("Could not determine initialization vector for cipher")
    }
    def toParameterSpec(paramMap: Map[String, String]): AlgorithmParameterSpec = {
      new GCMParameterSpec(paramMap("authenticationTagLength").toInt, base64Decode(paramMap(InitializationVectorProp)))
    }
  }
}
