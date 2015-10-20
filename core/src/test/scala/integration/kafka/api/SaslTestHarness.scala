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

import java.io.{BufferedReader, FileWriter, BufferedWriter, File}
import javax.security.auth.login.Configuration

import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.kafka.common.security.JaasUtils
import org.junit.{After, Before}

trait SaslTestHarness extends ZooKeeperTestHarness {
  val workDir = new File(System.getProperty("test.dir", "target"))
  val kdcConf = MiniKdc.createConf()
  val kdc = new MiniKdc(kdcConf, workDir)

  @Before
  override def setUp() {
    // Clean-up global configuration set by other tests
    Configuration.setConfiguration(null)
    val keytabFile = TestUtils.tempFile()
    val jaasFile = TestUtils.tempFile()

    val writer = new BufferedWriter(new FileWriter(jaasFile))
    val source = io.Source.fromInputStream(
      Thread.currentThread().getContextClassLoader.getResourceAsStream("kafka_jaas.conf"), "UTF-8")
    if (source == null)
      throw new IllegalStateException("Could not load `kaas_jaas.conf`, make sure it is in the classpath")

    for (line <- source.getLines) {
      val replaced = line.replaceAll("\\$keytab-location", keytabFile.getAbsolutePath)
      writer.write(replaced)
      writer.newLine()
    }
    writer.close()
    source.close()

    kdc.start()
    kdc.createPrincipal(keytabFile, "client", "kafka/localhost")
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile.getAbsolutePath)
    super.setUp
  }

  @After
  override def tearDown() {
    super.tearDown
    kdc.stop()
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    Configuration.setConfiguration(null)
  }
}
