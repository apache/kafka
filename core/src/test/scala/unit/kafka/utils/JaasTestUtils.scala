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

object JaasTestUtils {

  case class Krb5LoginModule(contextName: String,
                             useKeyTab: Boolean,
                             storeKey: Boolean,
                             keyTab: String,
                             principal: String,
                             debug: Boolean,
                             serviceName: Option[String]) {
    def toJaasSection: JaasSection = {
      JaasSection(
        contextName,
        "com.sun.security.auth.module.Krb5LoginModule",
        debug = debug,
        entries = Map(
          "useKeyTab" -> useKeyTab.toString,
          "storeKey" -> storeKey.toString,
          "keyTab" -> keyTab,
          "principal" -> principal
        ) ++ serviceName.map(s => Map("serviceName" -> s)).getOrElse(Map.empty)
      )
    }
  }

  case class JaasSection(contextName: String,
                         moduleName: String,
                         debug: Boolean,
                         entries: Map[String, String]) {
    override def toString: String = {
      s"""|$contextName {
          |  $moduleName required
          |  debug=$debug
          |  ${entries.map { case (k, v) => s"""$k="$v"""" }.mkString("", "\n|  ", ";")}
          |};
          |""".stripMargin
    }
  }

  private val ZkServerContextName = "Server"
  private val ZkClientContextName = "Client"
  private val ZkUserSuperPasswd = "adminpasswd"
  private val ZkUser = "fpj"
  private val ZkUserPassword = "fpjsecret"
  private val ZkModule = "org.apache.zookeeper.server.auth.DigestLoginModule"

  private val KafkaServerContextName = "KafkaServer"
  private val KafkaServerPrincipal = "kafka/localhost@EXAMPLE.COM"
  private val KafkaClientContextName = "KafkaClient"
  private val KafkaClientPrincipal = "client@EXAMPLE.COM"

  def writeZkFile(): String = {
    val jaasFile = TestUtils.tempFile()
    writeToFile(jaasFile, zkSections)
    jaasFile.getCanonicalPath
  }

  def writeKafkaFile(serverKeyTabLocation: File, clientKeyTabLocation: File): String = {
    val jaasFile = TestUtils.tempFile()
    writeToFile(jaasFile, kafkaSections(serverKeyTabLocation, clientKeyTabLocation))
    jaasFile.getCanonicalPath
  }

  def writeZkAndKafkaFiles(serverKeyTabLocation: File, clientKeyTabLocation: File): String = {
    val jaasFile = TestUtils.tempFile()
    writeToFile(jaasFile, kafkaSections(serverKeyTabLocation, clientKeyTabLocation) ++ zkSections)
    jaasFile.getCanonicalPath
  }

  private def zkSections: Seq[JaasSection] = Seq(
    JaasSection(ZkServerContextName, ZkModule, false, Map("user_super" -> ZkUserSuperPasswd, s"user_$ZkUser" -> ZkUserPassword)),
    JaasSection(ZkClientContextName, ZkModule, false, Map("username" -> ZkUser, "password" -> ZkUserPassword))
  )

  private def kafkaSections(serverKeytabLocation: File, clientKeytabLocation: File): Seq[JaasSection] = {
    Seq(
      Krb5LoginModule(
        KafkaServerContextName,
        useKeyTab = true,
        storeKey = true,
        keyTab = serverKeytabLocation.getAbsolutePath,
        principal = KafkaServerPrincipal,
        debug = true,
        serviceName = Some("kafka")),
      Krb5LoginModule(
        KafkaClientContextName,
        useKeyTab = true,
        storeKey = true,
        keyTab = clientKeytabLocation.getAbsolutePath,
        principal = KafkaClientPrincipal,
        debug = true,
        serviceName = Some("kafka")
      )
    ).map(_.toJaasSection)
  }

  private def jaasSectionsToString(jaasSections: Seq[JaasSection]): String =
    jaasSections.mkString

  private def writeToFile(file: File, jaasSections: Seq[JaasSection]) {
    val writer = new BufferedWriter(new FileWriter(file))
    try writer.write(jaasSectionsToString(jaasSections))
    finally writer.close()
  }

}
