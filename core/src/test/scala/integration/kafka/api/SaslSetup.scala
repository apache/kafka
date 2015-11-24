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

package kafka.api

import java.io.{File}
import javax.security.auth.login.Configuration

import kafka.utils.{JaasTestUtils,TestUtils}
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.kerberos.LoginManager

trait SaslSetup {
  protected val zkSaslEnabled: Boolean
  private val workDir = new File(System.getProperty("test.dir", "target"))
  private val kdcConf = MiniKdc.createConf()
  private val kdc = new MiniKdc(kdcConf, workDir)

  def startSasl() {
    // Important if tests leak consumers, producers or brokers
    LoginManager.closeAll()
    val keytabFile = createKeytabAndSetConfiguration()
    kdc.start()
    kdc.createPrincipal(keytabFile, "client", "kafka/localhost")
    if (zkSaslEnabled)
      System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
  }

  protected def createKeytabAndSetConfiguration(): File = {
    val (keytabFile, jaasFile) = createKeytabAndJaasFiles()
    // This will cause a reload of the Configuration singleton when `getConfiguration` is called
    Configuration.setConfiguration(null)
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile.getAbsolutePath)
    keytabFile
  }

  private def createKeytabAndJaasFiles(): (File, File) = {
    val keytabFile = TestUtils.tempFile()
    val jaasFileName = if (zkSaslEnabled)
        JaasTestUtils.genSingleFile(keytabFile.getAbsolutePath)
      else
        JaasTestUtils.genKafkaFile(keytabFile.getAbsolutePath)
    val jaasFile = new File(jaasFileName)

    (keytabFile, jaasFile)
  }

  def closeSasl() {
    kdc.stop()
    // Important if tests leak consumers, producers or brokers
    LoginManager.closeAll()
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    System.clearProperty("zookeeper.authProvider.1");
    Configuration.setConfiguration(null)
  }
}