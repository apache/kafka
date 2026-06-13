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

import kafka.security.JaasTestUtils
import kafka.security.minikdc.MiniKdc
import kafka.utils.TestUtils

/**
 * Extends SaslSetupFixtures with embedded Kerberos support backed by MiniKdc.
 */
trait SaslSetup extends SaslSetupFixtures {
  private val workDir = TestUtils.tempDir()
  private val kdcConf = MiniKdc.createConfig
  private var kdc: MiniKdc = _

  override protected def initializeKerberos(): Unit = {
    val (serverKeytabFile, clientKeytabFile) = maybeCreateEmptyKeytabFiles()
    kdc = new MiniKdc(kdcConf, workDir)
    kdc.start()
    kdc.createPrincipal(serverKeytabFile, java.util.List.of(JaasTestUtils.KAFKA_SERVER_PRINCIPAL_UNQUALIFIED_NAME + "/localhost"))
    kdc.createPrincipal(clientKeytabFile,
      java.util.List.of(JaasTestUtils.KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME, JaasTestUtils.KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME_2))
  }

  override protected def stopKerberos(): Unit = {
    if (kdc != null)
      kdc.stop()
  }
}
