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

import java.io.{File, BufferedWriter, FileWriter}
import java.util.Properties
import kafka.server.KafkaConfig
import org.apache.kafka.common.utils.Java

object JaasTestUtils {

  case class Krb5LoginModule(useKeyTab: Boolean,
                             storeKey: Boolean,
                             keyTab: String,
                             principal: String,
                             debug: Boolean,
                             serviceName: Option[String]) extends JaasModule {

    def name =
      if (Java.isIbmJdk)
        "com.ibm.security.auth.module.Krb5LoginModule"
      else
        "com.sun.security.auth.module.Krb5LoginModule"

    def entries: Map[String, String] =
      if (Java.isIbmJdk)
        Map(
          "principal" -> principal,
          "credsType" -> "both"
        ) ++ (if (useKeyTab) Map("useKeytab" -> s"file:$keyTab") else Map.empty)
      else
        Map(
          "useKeyTab" -> useKeyTab.toString,
          "storeKey" -> storeKey.toString,
          "keyTab" -> keyTab,
          "principal" -> principal
        ) ++ serviceName.map(s => Map("serviceName" -> s)).getOrElse(Map.empty)
  }

  case class PlainLoginModule(username: String,
                              password: String,
                              debug: Boolean = false,
                              validUsers: Map[String, String] = Map.empty) extends JaasModule {

    def name = "org.apache.kafka.common.security.plain.PlainLoginModule"

    def entries: Map[String, String] = Map(
      "username" -> username,
      "password" -> password
    ) ++ validUsers.map { case (user, pass) => s"user_$user" -> pass }

  }

  case class ZkDigestModule(debug: Boolean = false,
                            entries: Map[String, String] = Map.empty) extends JaasModule {
    def name = "org.apache.zookeeper.server.auth.DigestLoginModule"
  }

  case class ScramLoginModule(username: String,
                              password: String,
                              debug: Boolean = false) extends JaasModule {

    def name = "org.apache.kafka.common.security.scram.ScramLoginModule"

    def entries: Map[String, String] = Map(
      "username" -> username,
      "password" -> password
    )
  }

  sealed trait JaasModule {
    def name: String
    def debug: Boolean
    def entries: Map[String, String]

    override def toString: String = {
      s"""$name required
          |  debug=$debug
          |  ${entries.map { case (k, v) => s"""$k="$v"""" }.mkString("", "\n|  ", ";")}
          |""".stripMargin
    }
  }

  case class JaasSection(contextName: String, modules: Seq[JaasModule]) {
    override def toString: String = {
      s"""|$contextName {
          |  ${modules.mkString("\n  ")}
          |};
          |""".stripMargin
    }
  }

  private val ZkServerContextName = "Server"
  private val ZkClientContextName = "Client"
  private val ZkUserSuperPasswd = "adminpasswd"
  private val ZkUser = "fpj"
  private val ZkUserPassword = "fpjsecret"

  val KafkaServerContextName = "KafkaServer"
  val KafkaServerPrincipalUnqualifiedName = "kafka"
  private val KafkaServerPrincipal = KafkaServerPrincipalUnqualifiedName + "/localhost@EXAMPLE.COM"
  private val KafkaClientContextName = "KafkaClient"
  val KafkaClientPrincipalUnqualifiedName = "client"
  private val KafkaClientPrincipal = KafkaClientPrincipalUnqualifiedName + "@EXAMPLE.COM"
  val KafkaClientPrincipalUnqualifiedName2 = "client2"
  private val KafkaClientPrincipal2 = KafkaClientPrincipalUnqualifiedName2 + "@EXAMPLE.COM"
  
  val KafkaPlainUser = "plain-user"
  private val KafkaPlainPassword = "plain-user-secret"
  val KafkaPlainUser2 = "plain-user2"
  val KafkaPlainPassword2 = "plain-user2-secret"
  val KafkaPlainAdmin = "plain-admin"
  private val KafkaPlainAdminPassword = "plain-admin-secret"

  val KafkaScramUser = "scram-user"
  val KafkaScramPassword = "scram-user-secret"
  val KafkaScramUser2 = "scram-user2"
  val KafkaScramPassword2 = "scram-user2-secret"
  val KafkaScramAdmin = "scram-admin"
  val KafkaScramAdminPassword = "scram-admin-secret"

  val serviceName = "kafka"

  def saslConfigs(saslProperties: Option[Properties]): Properties = {
    val result = saslProperties match {
      case Some(properties) => properties
      case None => new Properties
    }
    // IBM Kerberos module doesn't support the serviceName JAAS property, hence it needs to be
    // passed as a Kafka property
    if (Java.isIbmJdk && !result.contains(KafkaConfig.SaslKerberosServiceNameProp))
      result.put(KafkaConfig.SaslKerberosServiceNameProp, serviceName)
    result
  }

  def writeJaasContextsToFile(jaasSections: Seq[JaasSection]): File = {
    val jaasFile = TestUtils.tempFile()
    writeToFile(jaasFile, jaasSections)
    jaasFile
  }

  // Returns the dynamic configuration, using credentials for user #1
  def clientLoginModule(mechanism: String, keytabLocation: Option[File]): String =
    kafkaClientModule(mechanism, keytabLocation, KafkaClientPrincipal, KafkaPlainUser, KafkaPlainPassword, KafkaScramUser, KafkaScramPassword).toString

  def zkSections: Seq[JaasSection] = Seq(
    JaasSection(ZkServerContextName, Seq(ZkDigestModule(debug = false,
      Map("user_super" -> ZkUserSuperPasswd, s"user_$ZkUser" -> ZkUserPassword)))),
    JaasSection(ZkClientContextName, Seq(ZkDigestModule(debug = false,
      Map("username" -> ZkUser, "password" -> ZkUserPassword))))
  )

  def kafkaServerSection(contextName: String, mechanisms: Seq[String], keytabLocation: Option[File]): JaasSection = {
    val modules = mechanisms.map {
      case "GSSAPI" =>
        Krb5LoginModule(
          useKeyTab = true,
          storeKey = true,
          keyTab = keytabLocation.getOrElse(throw new IllegalArgumentException("Keytab location not specified for GSSAPI")).getAbsolutePath,
          principal = KafkaServerPrincipal,
          debug = true,
          serviceName = Some(serviceName))
      case "PLAIN" =>
        PlainLoginModule(
          KafkaPlainAdmin,
          KafkaPlainAdminPassword,
          debug = false,
          Map(
            KafkaPlainAdmin -> KafkaPlainAdminPassword,
            KafkaPlainUser -> KafkaPlainPassword,
            KafkaPlainUser2 -> KafkaPlainPassword2
          ))
      case "SCRAM-SHA-256" | "SCRAM-SHA-512" =>
        ScramLoginModule(
          KafkaScramAdmin,
          KafkaScramAdminPassword,
          debug = false)
      case mechanism => throw new IllegalArgumentException("Unsupported server mechanism " + mechanism)
    }
    JaasSection(contextName, modules)
  }

  // consider refactoring if more mechanisms are added
  private def kafkaClientModule(mechanism: String, 
      keytabLocation: Option[File], clientPrincipal: String,
      plainUser: String, plainPassword: String, 
      scramUser: String, scramPassword: String): JaasModule = {
    mechanism match {
      case "GSSAPI" =>
        Krb5LoginModule(
          useKeyTab = true,
          storeKey = true,
          keyTab = keytabLocation.getOrElse(throw new IllegalArgumentException("Keytab location not specified for GSSAPI")).getAbsolutePath,
          principal = clientPrincipal,
          debug = true,
          serviceName = Some(serviceName)
        )
      case "PLAIN" =>
        PlainLoginModule(
          plainUser,
          plainPassword
        )
      case "SCRAM-SHA-256" | "SCRAM-SHA-512" =>
        ScramLoginModule(
          scramUser,
          scramPassword
        )
      case mechanism => throw new IllegalArgumentException("Unsupported client mechanism " + mechanism)
    }
  }

  /*
   * Used for the static JAAS configuration and it uses the credentials for client#2
   */
  def kafkaClientSection(mechanism: Option[String], keytabLocation: Option[File]): JaasSection = {
    JaasSection(KafkaClientContextName, mechanism.map(m =>
      kafkaClientModule(m, keytabLocation, KafkaClientPrincipal2, KafkaPlainUser2, KafkaPlainPassword2, KafkaScramUser2, KafkaScramPassword2)).toSeq)
  }

  private def jaasSectionsToString(jaasSections: Seq[JaasSection]): String =
    jaasSections.mkString

  private def writeToFile(file: File, jaasSections: Seq[JaasSection]) {
    val writer = new BufferedWriter(new FileWriter(file))
    try writer.write(jaasSectionsToString(jaasSections))
    finally writer.close()
  }

}
