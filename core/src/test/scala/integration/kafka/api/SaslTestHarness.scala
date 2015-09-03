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
import java.net.URL
import javax.security.auth.login.Configuration

import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.hadoop.minikdc.MiniKdc
import org.junit.{After, Before}
import org.scalatest.junit.JUnitSuite


trait SaslTestHarness extends ZooKeeperTestHarness {
  val WorkDir = new File(System.getProperty("test.dir", "target"));
  val KdcConf = MiniKdc.createConf();
  val Kdc: MiniKdc = new MiniKdc(KdcConf, WorkDir);

  @Before
  override def setUp() {
    val keytabFile: File = TestUtils.tempFile()
    val jaasFile: File = TestUtils.tempFile()

    val writer: BufferedWriter = new BufferedWriter(new FileWriter(jaasFile))

    val path: String = Thread.currentThread().getContextClassLoader.getResource("kafka_jaas.conf").getPath
    for(line <- io.Source.fromFile(path).getLines()) {
      val s: String = "\\$keytab-location"
      val replaced = line.replaceAll("\\$keytab-location",keytabFile.getAbsolutePath)
      writer.write(replaced)
      writer.newLine()
    }
    writer.close()

    Kdc.start()
    Kdc.createPrincipal(keytabFile, "client", "kafka/localhost")
    System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath)
    super.setUp
  }

  @After
  override def tearDown() {
    super.tearDown
    Kdc.stop()
    System.clearProperty("java.security.auth.login.config")
    Configuration.setConfiguration(null)
  }
}
