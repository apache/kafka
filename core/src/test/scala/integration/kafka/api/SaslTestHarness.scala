/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.io.{FileWriter, BufferedWriter, File}
import javax.security.auth.login.Configuration

import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.kerberos.LoginManager
import org.junit.{After, Before}

trait SaslTestHarness extends ZooKeeperTestHarness {

  private val workDir = new File(System.getProperty("test.dir", "target"))
  private val kdcConf = MiniKdc.createConf()
  private val kdc = new MiniKdc(kdcConf, workDir)

  @Before
  override def setUp() {
    // Important if tests leak consumers, producers or brokers
    LoginManager.closeAll()
    val keytabFile = createKeytabAndSetConfiguration("kafka_jaas.conf")
    kdc.start()
    kdc.createPrincipal(keytabFile, "client", "kafka/localhost")
    super.setUp
  }

  protected def createKeytabAndSetConfiguration(jaasResourceName: String): File = {
    val (keytabFile, jaasFile) = createKeytabAndJaasFiles(jaasResourceName)
    // This will cause a reload of the Configuration singleton when `getConfiguration` is called
    Configuration.setConfiguration(null)
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile.getAbsolutePath)
    keytabFile
  }

  private def createKeytabAndJaasFiles(jaasResourceName: String): (File, File) = {
    val keytabFile = TestUtils.tempFile()
    val jaasFile = TestUtils.tempFile()

    val writer = new BufferedWriter(new FileWriter(jaasFile))
    val inputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(jaasResourceName)
    if (inputStream == null)
      throw new IllegalStateException(s"Could not find `$jaasResourceName`, make sure it is in the classpath")
    val source = io.Source.fromInputStream(inputStream, "UTF-8")

    for (line <- source.getLines) {
      val replaced = line.replaceAll("\\$keytab-location", keytabFile.getAbsolutePath)
      writer.write(replaced)
      writer.newLine()
    }

    writer.close()
    source.close()

    (keytabFile, jaasFile)
  }

  @After
  override def tearDown() {
    super.tearDown
    kdc.stop()
    // Important if tests leak consumers, producers or brokers
    LoginManager.closeAll()
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    Configuration.setConfiguration(null)
  }

}
