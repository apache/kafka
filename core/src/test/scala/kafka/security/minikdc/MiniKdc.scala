/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.security.minikdc

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.text.MessageFormat
import java.util.{Locale, Properties, UUID}
import kafka.utils.{CoreUtils, Exit, Logging}
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory

import scala.jdk.CollectionConverters._
import org.apache.kerby.kerberos.kerb.KrbException
import org.apache.kerby.kerberos.kerb.identity.backend.BackendConfig
import org.apache.kerby.kerberos.kerb.server.{KdcConfig, KdcConfigKey, SimpleKdcServer}
import org.apache.kafka.common.utils.{Java, Utils}
import org.apache.kerby.kerberos.kerb.`type`.KerberosTime
import org.apache.kerby.kerberos.kerb.`type`.base.{EncryptionKey, PrincipalName}
import org.apache.kerby.kerberos.kerb.keytab.{Keytab, KeytabEntry}
import org.apache.kerby.util.NetworkUtil
/**
  * Mini KDC based on Apache Directory Server that can be embedded in tests or used from command line as a standalone
  * KDC.
  *
  * MiniKdc sets 2 System properties when started and unsets them when stopped:
  *
  * - java.security.krb5.conf: set to the MiniKDC real/host/port
  * - sun.security.krb5.debug: set to the debug value provided in the configuration
  *
  * As a result of this, multiple MiniKdc instances should not be started concurrently in the same JVM.
  *
  * MiniKdc default configuration values are:
  *
  * - org.name=EXAMPLE (used to create the REALM)
  * - org.domain=COM (used to create the REALM)
  * - kdc.bind.address=localhost
  * - kdc.port=0 (ephemeral port)
  * - instance=DefaultKrbServer
  * - max.ticket.lifetime=86400000 (1 day)
  * - max.renewable.lifetime604800000 (7 days)
  * - transport=TCP
  * - debug=false
  *
  * The generated krb5.conf forces TCP connections.
  *
  * Acknowledgements: this class is derived from the MiniKdc class in the hadoop-minikdc project (git commit
  * d8d8ed35f00b15ee0f2f8aaf3fe7f7b42141286b).
  *
  * @constructor creates a new MiniKdc instance.
  * @param config the MiniKdc configuration
  * @param workDir the working directory which will contain krb5.conf, Apache DS files and any other files needed by
  *                MiniKdc.
  * @throws Exception thrown if the MiniKdc could not be created.
  */
class MiniKdc(config: Properties, workDir: File) extends Logging {

  if (!config.keySet.containsAll(MiniKdc.RequiredProperties.asJava)) {
    val missingProperties = MiniKdc.RequiredProperties.filterNot(config.keySet.asScala)
    throw new IllegalArgumentException(s"Missing configuration properties: $missingProperties")
  }

  info("Configuration:")
  info("---------------------------------------------------------------")
  config.forEach { (key, value) =>
    info(s"\t$key: $value")
  }
  info("---------------------------------------------------------------")

  private val orgName = config.getProperty(MiniKdc.OrgName)
  private val orgDomain = config.getProperty(MiniKdc.OrgDomain)
  private val realm = s"${orgName.toUpperCase(Locale.ENGLISH)}.${orgDomain.toUpperCase(Locale.ENGLISH)}"
  private val krb5conf = new File(workDir, "krb5.conf")

  private var _port = config.getProperty(MiniKdc.KdcPort).toInt
  private var kdc: SimpleKdcServer = _
  private var closed = false

  def port: Int = _port

  def host: String = config.getProperty(MiniKdc.KdcBindAddress)

  def start(): Unit = {
    if (kdc != null)
      throw new RuntimeException("KDC already started")
    if (closed)
      throw new RuntimeException("KDC is closed")
    initKdcServer()
    initJvmKerberosConfig()
  }

  private def initKdcServer(): Unit = {
    val kdcConfig = new KdcConfig()
    kdcConfig.setLong(KdcConfigKey.MAXIMUM_RENEWABLE_LIFETIME, config.getProperty(MiniKdc.MaxRenewableLifetime).toLong)
    kdcConfig.setLong(KdcConfigKey.MAXIMUM_TICKET_LIFETIME,
      config.getProperty(MiniKdc.MaxTicketLifetime).toLong)
    kdcConfig.setString(KdcConfigKey.KDC_REALM, realm)
    kdcConfig.setString(KdcConfigKey.KDC_HOST, host)
    kdcConfig.setBoolean(KdcConfigKey.PA_ENC_TIMESTAMP_REQUIRED, false)
    kdcConfig.setString(KdcConfigKey.KDC_SERVICE_NAME, config.getProperty(MiniKdc.Instance))
    kdc = new SimpleKdcServer(kdcConfig, new BackendConfig)
    kdc.setWorkDir(workDir)
    // if using ephemeral port, update port number for binding
    if (port == 0)
      _port = NetworkUtil.getServerPort

    val transport = config.getProperty(MiniKdc.Transport)
    transport.trim match {
      case "TCP" =>
        kdc.setKdcTcpPort(port)
        kdc.setAllowUdp(false)
        kdc.setAllowTcp(true)
      case "UDP" =>
        kdc.setKdcUdpPort(port)
        kdc.setAllowTcp(false)
        kdc.setAllowUdp(true)
      case _ => throw new IllegalArgumentException(s"Invalid transport: $transport")
    }

    kdc.init()
    kdc.start()

    info(s"MiniKdc listening at port: $port")
  }

  private def initJvmKerberosConfig(): Unit = {
    writeKrb5Conf()
    System.setProperty(MiniKdc.JavaSecurityKrb5Conf, krb5conf.getAbsolutePath)
    System.setProperty(MiniKdc.SunSecurityKrb5Debug, config.getProperty(MiniKdc.Debug, "false"))
    info(s"MiniKdc setting JVM krb5.conf to: ${krb5conf.getAbsolutePath}")
    refreshJvmKerberosConfig()
  }

  private def writeKrb5Conf(): Unit = {
    val stringBuilder = new StringBuilder
    val reader = new BufferedReader(
      new InputStreamReader(MiniKdc.getResourceAsStream("minikdc-krb5.conf"), StandardCharsets.UTF_8))
    try {
      var line: String = null
      while ({line = reader.readLine(); line != null}) {
        stringBuilder.append(line).append("{3}")
      }
    } finally CoreUtils.swallow(reader.close(), this)
    val output = MessageFormat.format(stringBuilder.toString, realm, host, port.toString, System.lineSeparator())
    Files.write(krb5conf.toPath, output.getBytes(StandardCharsets.UTF_8))
  }

  private def refreshJvmKerberosConfig(): Unit = {
    val klass =
      if (Java.isIbmJdk && !Java.isIbmJdkSemeru)
        Class.forName("com.ibm.security.krb5.internal.Config")
      else
        Class.forName("sun.security.krb5.Config")
    klass.getMethod("refresh").invoke(klass)
  }

  def stop(): Unit = {
    if (!closed) {
      closed = true
      if (kdc != null) {
        System.clearProperty(MiniKdc.JavaSecurityKrb5Conf)
        System.clearProperty(MiniKdc.SunSecurityKrb5Debug)

        try {
          kdc.stop()
          info("KDC server stopped")
        } catch {
          case ex: Exception => error("Could not stop the KDC server properly", ex)
        }
      }
    }
  }

  /**
    * Creates  multiple principals in the KDC and adds them to a keytab file.
    *
    * An exception will be thrown if the principal cannot be created.
    *
    * @param keytabFile keytab file to add the created principals
    * @param principals principals to add to the KDC, do not include the domain.
    */
  def createPrincipal(keytabFile: File, principals: String*): Unit = {
    try {
      val keytab = new Keytab
      val generatedPassword = UUID.randomUUID.toString
      val entries = principals.flatMap { principal =>
        val principalWithRealm = s"${principal}@${realm}"
        kdc.getKadmin.addPrincipal(principalWithRealm, generatedPassword)
        KerberosKeyFactory.getKerberosKeys(principalWithRealm, generatedPassword)
          .asScala.values
          .map { encryptionKey =>
            val keyVersion = encryptionKey.getKeyVersion
            val timestamp = new KerberosTime()
            val principalName = new PrincipalName(principalWithRealm)
            val key = new EncryptionKey(encryptionKey.getKeyType.getValue, encryptionKey.getKeyValue)
            new KeytabEntry(principalName, timestamp, keyVersion, key)
          }
      }
      keytab.addKeytabEntries(entries.asJava)
      keytab.store(keytabFile)
      info(s"Keytab file created at ${keytabFile.getAbsolutePath}")
    } catch {
      case e: KrbException =>
        error("Error occurred while exporting keytab", e)
    }
  }
}

object MiniKdc {

  val JavaSecurityKrb5Conf = "java.security.krb5.conf"
  val SunSecurityKrb5Debug = "sun.security.krb5.debug"

  def main(args: Array[String]): Unit = {
    args match {
      case Array(workDirPath, configPath, keytabPath, principals@ _*) if principals.nonEmpty =>
        val workDir = new File(workDirPath)
        if (!workDir.exists)
          throw new RuntimeException(s"Specified work directory does not exist: ${workDir.getAbsolutePath}")
        val config = createConfig
        val configFile = new File(configPath)
        if (!configFile.exists)
          throw new RuntimeException(s"Specified configuration does not exist: ${configFile.getAbsolutePath}")

        val userConfig = Utils.loadProps(configFile.getAbsolutePath)
        userConfig.forEach { (key, value) =>
          config.put(key, value)
        }
        val keytabFile = new File(keytabPath).getAbsoluteFile
        start(workDir, config, keytabFile, principals)
      case _ =>
        println("Arguments: <WORKDIR> <MINIKDCPROPERTIES> <KEYTABFILE> [<PRINCIPALS>]+")
        Exit.exit(1)
    }
  }

  private[minikdc] def start(workDir: File, config: Properties, keytabFile: File, principals: Seq[String]): MiniKdc = {
    val miniKdc = new MiniKdc(config, workDir)
    miniKdc.start()
    miniKdc.createPrincipal(keytabFile, principals: _*)
    val infoMessage = s"""
      |
      |Standalone MiniKdc Running
      |---------------------------------------------------
      |  Realm           : ${miniKdc.realm}
      |  Running at      : ${miniKdc.host}:${miniKdc.port}
      |  krb5conf        : ${miniKdc.krb5conf}
      |
      |  created keytab  : $keytabFile
      |  with principals : ${principals.mkString(", ")}
      |
      |Hit <CTRL-C> or kill <PID> to stop it
      |---------------------------------------------------
      |
    """.stripMargin
    println(infoMessage)
    Exit.addShutdownHook("minikdc-shutdown-hook", miniKdc.stop())
    miniKdc
  }

  val OrgName = "org.name"
  val OrgDomain = "org.domain"
  val KdcBindAddress = "kdc.bind.address"
  val KdcPort = "kdc.port"
  val Instance = "instance"
  val MaxTicketLifetime = "max.ticket.lifetime"
  val MaxRenewableLifetime = "max.renewable.lifetime"
  val Transport = "transport"
  val Debug = "debug"

  private val RequiredProperties = Set(OrgName, OrgDomain, KdcBindAddress, KdcPort, Instance, Transport,
    MaxTicketLifetime, MaxRenewableLifetime)

  private val DefaultConfig = Map(
    KdcBindAddress -> "localhost",
    KdcPort -> "0",
    Instance -> "DefaultKrbServer",
    OrgName -> "Example",
    OrgDomain -> "COM",
    Transport -> "TCP",
    MaxTicketLifetime -> "86400000",
    MaxRenewableLifetime -> "604800000",
    Debug -> "false"
  )

  /**
    * Convenience method that returns MiniKdc default configuration.
    *
    * The returned configuration is a copy, it can be customized before using
    * it to create a MiniKdc.
    */
  def createConfig: Properties = {
    val properties = new Properties
    DefaultConfig.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  @throws[IOException]
  def getResourceAsStream(resourceName: String): InputStream = {
    val cl = Option(Thread.currentThread.getContextClassLoader).getOrElse(classOf[MiniKdc].getClassLoader)
    Option(cl.getResourceAsStream(resourceName)).getOrElse {
      throw new IOException(s"Can not read resource file `$resourceName`")
    }
  }

}
