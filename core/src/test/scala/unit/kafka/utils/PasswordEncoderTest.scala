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


import javax.crypto.SecretKeyFactory
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.server.config.Defaults
import org.apache.kafka.utils.PasswordEncoder
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class PasswordEncoderTest {

  @Test
  def testEncodeDecode(): Unit = {
    val encoder = PasswordEncoder.encrypting(new Password("password-encoder-secret"),
      null,
      Defaults.PASSWORD_ENCODER_CIPHER_ALGORITHM,
      Defaults.PASSWORD_ENCODER_KEY_LENGTH,
      Defaults.PASSWORD_ENCODER_ITERATIONS)
    val password = "test-password"
    val encoded = encoder.encode(new Password(password))
    val encodedMap = CoreUtils.parseCsvMap(encoded)
    assertEquals("4096", encodedMap(PasswordEncoder.ITERATIONS_PROP))
    assertEquals("128", encodedMap(PasswordEncoder.KEY_LENGTH_PROP))
    val defaultKeyFactoryAlgorithm = try {
      SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512")
      "PBKDF2WithHmacSHA512"
    } catch {
      case _: Exception => "PBKDF2WithHmacSHA1"
    }
    assertEquals(defaultKeyFactoryAlgorithm, encodedMap(PasswordEncoder.KEY_FACTORY_ALGORITHM_PROP))
    assertEquals("AES/CBC/PKCS5Padding", encodedMap(PasswordEncoder.CIPHER_ALGORITHM_PROP))

    verifyEncodedPassword(encoder, password, encoded)
  }

  @Test
  def testEncoderConfigChange(): Unit = {
    val encoder = PasswordEncoder.encrypting(new Password("password-encoder-secret"),
      "PBKDF2WithHmacSHA1",
      "DES/CBC/PKCS5Padding",
      64,
      1024)
    val password = "test-password"
    val encoded = encoder.encode(new Password(password))
    val encodedMap = CoreUtils.parseCsvMap(encoded)
    assertEquals("1024", encodedMap(PasswordEncoder.ITERATIONS_PROP))
    assertEquals("64", encodedMap(PasswordEncoder.KEY_LENGTH_PROP))
    assertEquals("PBKDF2WithHmacSHA1", encodedMap(PasswordEncoder.KEY_FACTORY_ALGORITHM_PROP))
    assertEquals("DES/CBC/PKCS5Padding", encodedMap(PasswordEncoder.CIPHER_ALGORITHM_PROP))

    // Test that decoding works even if PasswordEncoder algorithm, iterations etc. are altered
    val decoder = PasswordEncoder.encrypting(new Password("password-encoder-secret"),
      "PBKDF2WithHmacSHA1",
      "AES/CBC/PKCS5Padding",
      128,
      2048)
    assertEquals(password, decoder.decode(encoded).value)

    // Test that decoding fails if secret is altered
    val decoder2 = PasswordEncoder.encrypting(new Password("secret-2"),
      "PBKDF2WithHmacSHA1",
      "AES/CBC/PKCS5Padding",
      128,
      1024)
    try {
      decoder2.decode(encoded)
    } catch {
      case e: ConfigException => // expected exception
    }
  }

  @Test
  def testEncodeDecodeAlgorithms(): Unit = {

    def verifyEncodeDecode(keyFactoryAlg: String, cipherAlg: String, keyLength: Int): Unit = {
      val encoder = PasswordEncoder.encrypting(new Password("password-encoder-secret"),
        keyFactoryAlg,
        cipherAlg,
        keyLength,
        Defaults.PASSWORD_ENCODER_ITERATIONS)
      val password = "test-password"
      val encoded = encoder.encode(new Password(password))
      verifyEncodedPassword(encoder, password, encoded)
    }

    verifyEncodeDecode(keyFactoryAlg = null, "DES/CBC/PKCS5Padding", keyLength = 64)
    verifyEncodeDecode(keyFactoryAlg = null, "DESede/CBC/PKCS5Padding", keyLength = 192)
    verifyEncodeDecode(keyFactoryAlg = null, "AES/CBC/PKCS5Padding", keyLength = 128)
    verifyEncodeDecode(keyFactoryAlg = null, "AES/CFB/PKCS5Padding", keyLength = 128)
    verifyEncodeDecode(keyFactoryAlg = null, "AES/OFB/PKCS5Padding", keyLength = 128)
    verifyEncodeDecode(keyFactoryAlg = "PBKDF2WithHmacSHA1", Defaults.PASSWORD_ENCODER_CIPHER_ALGORITHM, keyLength = 128)
    verifyEncodeDecode(keyFactoryAlg = null, "AES/GCM/NoPadding", keyLength = 128)
    verifyEncodeDecode(keyFactoryAlg = "PBKDF2WithHmacSHA256", Defaults.PASSWORD_ENCODER_CIPHER_ALGORITHM, keyLength = 128)
    verifyEncodeDecode(keyFactoryAlg = "PBKDF2WithHmacSHA512", Defaults.PASSWORD_ENCODER_CIPHER_ALGORITHM, keyLength = 128)
  }

  private def verifyEncodedPassword(encoder: PasswordEncoder, password: String, encoded: String): Unit = {
    val encodedMap = CoreUtils.parseCsvMap(encoded)
    assertEquals(password.length.toString, encodedMap(PasswordEncoder.PASSWORD_LENGTH_PROP))
    assertNotNull(PasswordEncoder.base64Decode(encodedMap("salt")), "Invalid salt")
    assertNotNull(PasswordEncoder.base64Decode(encodedMap(PasswordEncoder.INITIALIZATION_VECTOR_PROP)), "Invalid encoding parameters")
    assertNotNull(PasswordEncoder.base64Decode(encodedMap(PasswordEncoder.ENCRYPTED_PASSWORD_PROP)), "Invalid encoded password")
    assertEquals(password, encoder.decode(encoded).value)
  }
}
