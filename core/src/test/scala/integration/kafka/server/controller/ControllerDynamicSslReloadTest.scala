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

package kafka.server.controller

import kafka.server.QuorumTestHarness
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.config.{ConfigResource, SslConfigs}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.test.TestSslUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo, Timeout}

import java.io.File
import java.security.cert.X509Certificate
import java.util.{Properties, Collections => JCollections}
import javax.net.ssl.{SSLContext, SSLSocket, TrustManager, X509TrustManager}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
 * Integration test for dynamic SSL reload on the CONTROLLER listener.
 *
 * Boots an isolated controller with SSL on the CONTROLLER listener using keystore A.
 * Issues an `IncrementalAlterConfigs` against `ConfigResource.Type.CONTROLLER` pointing
 * the listener at keystore B. Opens a fresh TLS connection and asserts that the certificate
 * presented on the wire is now from keystore B.
 */
@Timeout(120)
class ControllerDynamicSslReloadTest extends QuorumTestHarness {

  private var keystoreA: File = _
  private var keystoreB: File = _
  private var truststore: File = _
  private var serialA: java.math.BigInteger = _
  private var serialB: java.math.BigInteger = _

  private val KeystorePassword = "controller-test-password"
  private val KeyPassword = "controller-test-password"
  private val TruststorePassword = "controller-test-password"

  override protected val controllerListenerSecurityProtocol: SecurityProtocol = SecurityProtocol.SSL

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {

    keystoreA = newKeystore("keyA")
    keystoreB = newKeystore("keyB")
    truststore = newTruststore("server-trust", List(("a", keystoreA), ("b", keystoreB)))
    serialA = readSerial(keystoreA, "server")
    serialB = readSerial(keystoreB, "server")
    super.setUp(testInfo)
  }

  @AfterEach
  override def tearDown(): Unit = {
    try super.tearDown()
    finally Seq(keystoreA, keystoreB, truststore).filter(_ != null).foreach(f => Utils.delete(f))
  }

  override protected def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    val props = new Properties()
    val listenerPrefix = "listener.name.controller."
    props.setProperty(listenerPrefix + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreA.getAbsolutePath)
    props.setProperty(listenerPrefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KeystorePassword)
    props.setProperty(listenerPrefix + SslConfigs.SSL_KEY_PASSWORD_CONFIG, KeyPassword)
    props.setProperty(listenerPrefix + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore.getAbsolutePath)
    props.setProperty(listenerPrefix + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TruststorePassword)
    props.setProperty(listenerPrefix + "ssl.client.auth", "none")
    Seq(props)
  }

  @Test
  def testControllerListenerSslHotReload(): Unit = {
    val controllerListener = new ListenerName("CONTROLLER")
    val controllerPort = controllerServer.socketServer.boundPort(controllerListener)
    val nodeId = controllerServer.config.nodeId
    val baselineSerial = peerCertSerial("localhost", controllerPort)
    assertEquals(serialA, baselineSerial,
      s"Baseline cert on CONTROLLER listener should be keystore A's serial (expected=$serialA, got=$baselineSerial)")

    val admin = adminClient(controllerPort)
    try {
      val resource = new ConfigResource(ConfigResource.Type.CONTROLLER, nodeId.toString)
      val ops = List(
        new AlterConfigOp(new ConfigEntry("listener.name.controller." + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreB.getAbsolutePath), AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry("listener.name.controller." + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KeystorePassword), AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry("listener.name.controller." + SslConfigs.SSL_KEY_PASSWORD_CONFIG, KeyPassword), AlterConfigOp.OpType.SET)
      ).asJava
      admin.incrementalAlterConfigs(JCollections.singletonMap(resource, ops)).all().get(30, java.util.concurrent.TimeUnit.SECONDS)
    } finally {
      admin.close()
    }

    val deadline = System.currentTimeMillis() + 15.seconds.toMillis
    var observed: java.math.BigInteger = null
    while (System.currentTimeMillis() < deadline && observed != serialB) {
      observed = peerCertSerial("localhost", controllerPort)
      if (observed != serialB) Thread.sleep(200)
    }
    assertEquals(serialB, observed,
      s"After --alter --entity-type controllers, CONTROLLER listener should serve keystore B's cert " +
      s"(expected=$serialB, observed=$observed). The reload did not propagate to the SslChannelBuilder.")
  }

  private def adminClient(controllerPort: Int): Admin = {
    val props = new Properties()
    props.setProperty(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, s"localhost:$controllerPort")
    props.setProperty("security.protocol", SecurityProtocol.SSL.name)
    props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore.getAbsolutePath)
    props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TruststorePassword)
    props.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
    Admin.create(props)
  }

  private def peerCertSerial(host: String, port: Int): java.math.BigInteger = {
    val ctx = SSLContext.getInstance("TLS")
    val trustAll = Array[TrustManager](new X509TrustManager {
      override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
      override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
      override def getAcceptedIssuers: Array[X509Certificate] = Array.empty
    })
    ctx.init(null, trustAll, new java.security.SecureRandom())
    val socket = ctx.getSocketFactory.createSocket(host, port).asInstanceOf[SSLSocket]
    try {
      socket.startHandshake()
      val certs = socket.getSession.getPeerCertificates
      assertTrue(certs != null && certs.nonEmpty, "No peer cert presented")
      certs(0).asInstanceOf[X509Certificate].getSerialNumber
    } finally {
      socket.close()
    }
  }

  private def newKeystore(alias: String): File = {
    val file = java.io.File.createTempFile(s"ctrl-test-$alias-", ".jks")
    file.deleteOnExit()
    val keyPair = TestSslUtils.generateKeyPair("RSA")
    val cert = new TestSslUtils.CertificateBuilder()
      .sanDnsNames("localhost")
      .generate("CN=localhost", keyPair)
    TestSslUtils.createKeyStore(file.getAbsolutePath, new org.apache.kafka.common.config.types.Password(KeystorePassword),
      new org.apache.kafka.common.config.types.Password(KeyPassword), "server", keyPair.getPrivate, cert)
    file
  }

  private def newTruststore(name: String, sources: List[(String, File)]): File = {
    val file = java.io.File.createTempFile(s"ctrl-test-$name-", ".jks")
    file.deleteOnExit()
    val ts = java.security.KeyStore.getInstance("JKS")
    ts.load(null, null)
    sources.foreach { case (alias, ks) =>
      val src = java.security.KeyStore.getInstance("JKS")
      val in = new java.io.FileInputStream(ks)
      try src.load(in, KeystorePassword.toCharArray) finally in.close()
      ts.setCertificateEntry(alias, src.getCertificate("server"))
    }
    val out = new java.io.FileOutputStream(file)
    try ts.store(out, TruststorePassword.toCharArray) finally out.close()
    file
  }

  private def readSerial(keystore: File, alias: String): java.math.BigInteger = {
    val ks = java.security.KeyStore.getInstance("JKS")
    val in = new java.io.FileInputStream(keystore)
    try ks.load(in, KeystorePassword.toCharArray) finally in.close()
    ks.getCertificate(alias).asInstanceOf[X509Certificate].getSerialNumber
  }
}
