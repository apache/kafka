/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.security.minikdc

import java.util.Properties

import kafka.utils.TestUtils
import org.junit.Test
import org.junit.Assert._

class MiniKdcTest {
  @Test
  def shouldNotStopImmediatelyWhenStarted(): Unit = {
    val config = new Properties()
    config.setProperty("kdc.bind.address", "0.0.0.0")
    config.setProperty("transport", "TCP");
    config.setProperty("max.ticket.lifetime", "86400000")
    config.setProperty("org.name", "Example")
    config.setProperty("kdc.port", "0")
    config.setProperty("org.domain", "COM")
    config.setProperty("max.renewable.lifetime", "604800000")
    config.setProperty("instance", "DefaultKrbServer")
    val minikdc = MiniKdc.start(TestUtils.tempDir(), config, TestUtils.tempFile(), List("foo"))
    val running = System.getProperty(MiniKdc.JavaSecurityKrb5Conf) != null
    try {
      assertTrue("MiniKdc stopped immediately; it should not have", running)
    } finally {
      if (running) minikdc.stop()
    }
  }
}