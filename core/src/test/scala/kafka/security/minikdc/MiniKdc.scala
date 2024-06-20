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
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.text.MessageFormat
import java.util.{Locale, Properties, UUID}

import kafka.utils.{CoreUtils, Exit, Logging}

import scala.jdk.CollectionConverters._
import org.apache.commons.lang.text.StrSubstitutor
import org.apache.directory.api.ldap.model.entry.{DefaultEntry, Entry}
import org.apache.directory.api.ldap.model.ldif.LdifReader
import org.apache.directory.api.ldap.model.name.Dn
import org.apache.directory.api.ldap.schema.extractor.impl.DefaultSchemaLdifExtractor
import org.apache.directory.api.ldap.schema.loader.LdifSchemaLoader
import org.apache.directory.api.ldap.schema.manager.impl.DefaultSchemaManager
import org.apache.directory.server.constants.ServerDNConstants
import org.apache.directory.server.core.DefaultDirectoryService
import org.apache.directory.server.core.api.{CacheService, DirectoryService, InstanceLayout}
import org.apache.directory.server.core.api.schema.SchemaPartition
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor
import org.apache.directory.server.core.partition.impl.btree.jdbm.{JdbmIndex, JdbmPartition}
import org.apache.directory.server.core.partition.ldif.LdifPartition
import org.apache.directory.server.kerberos.KerberosConfig
import org.apache.directory.server.kerberos.kdc.KdcServer
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory
import org.apache.directory.server.kerberos.shared.keytab.{Keytab, KeytabEntry}
import org.apache.directory.server.protocol.shared.transport.{TcpTransport, UdpTransport}
import org.apache.directory.server.xdbm.Index
import org.apache.directory.shared.kerberos.KerberosTime
import org.apache.kafka.common.utils.{Java, Utils}

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
  private var ds: DirectoryService = _
  private var kdc: KdcServer = _
  private var closed = false

  def port: Int = _port

  def host: String = config.getProperty(MiniKdc.KdcBindAddress)

  def start(): Unit = {
    if (kdc != null)
      throw new RuntimeException("KDC already started")
    if (closed)
      throw new RuntimeException("KDC is closed")
    initDirectoryService()
    initKdcServer()
    initJvmKerberosConfig()
  }

  private def initDirectoryService(): Unit = {
    ds = new DefaultDirectoryService
    ds.setInstanceLayout(new InstanceLayout(workDir))
    ds.setCacheService(new CacheService)

    // first load the schema
    val instanceLayout = ds.getInstanceLayout
    val schemaPartitionDirectory = new File(instanceLayout.getPartitionsDirectory, "schema")
    val extractor = new DefaultSchemaLdifExtractor(instanceLayout.getPartitionsDirectory)
    extractor.extractOrCopy()

    val loader = new LdifSchemaLoader(schemaPartitionDirectory)
    val schemaManager = new DefaultSchemaManager(loader)
    schemaManager.loadAllEnabled()
    ds.setSchemaManager(schemaManager)
    // Init the LdifPartition with schema
    val schemaLdifPartition = new LdifPartition(schemaManager, ds.getDnFactory)
    schemaLdifPartition.setPartitionPath(schemaPartitionDirectory.toURI)

    // The schema partition
    val schemaPartition = new SchemaPartition(schemaManager)
    schemaPartition.setWrappedPartition(schemaLdifPartition)
    ds.setSchemaPartition(schemaPartition)

    val systemPartition = new JdbmPartition(ds.getSchemaManager, ds.getDnFactory)
    systemPartition.setId("system")
    systemPartition.setPartitionPath(new File(ds.getInstanceLayout.getPartitionsDirectory, systemPartition.getId).toURI)
    systemPartition.setSuffixDn(new Dn(ServerDNConstants.SYSTEM_DN))
    systemPartition.setSchemaManager(ds.getSchemaManager)
    ds.setSystemPartition(systemPartition)

    ds.getChangeLog.setEnabled(false)
    ds.setDenormalizeOpAttrsEnabled(true)
    ds.addLast(new KeyDerivationInterceptor)

    // create one partition
    val orgName = config.getProperty(MiniKdc.OrgName).toLowerCase(Locale.ENGLISH)
    val orgDomain = config.getProperty(MiniKdc.OrgDomain).toLowerCase(Locale.ENGLISH)
    val partition = new JdbmPartition(ds.getSchemaManager, ds.getDnFactory)
    partition.setId(orgName)
    partition.setPartitionPath(new File(ds.getInstanceLayout.getPartitionsDirectory, orgName).toURI)
    val dn = new Dn(s"dc=$orgName,dc=$orgDomain")
    partition.setSuffixDn(dn)
    ds.addPartition(partition)

    // indexes
    val indexedAttributes = Set[Index[_, String]](
      new JdbmIndex[Entry]("objectClass", false),
      new JdbmIndex[Entry]("dc", false),
      new JdbmIndex[Entry]("ou", false)
    ).asJava
    partition.setIndexedAttributes(indexedAttributes)

    // And start the ds
    ds.setInstanceId(config.getProperty(MiniKdc.Instance))
    ds.setShutdownHookEnabled(false)
    ds.startup()

    // context entry, after ds.startup()
    val entry = ds.newEntry(dn)
    entry.add("objectClass", "top", "domain")
    entry.add("dc", orgName)
    ds.getAdminSession.add(entry)
  }

  private def initKdcServer(): Unit = {

    def addInitialEntriesToDirectoryService(bindAddress: String): Unit = {
      val map = Map (
        "0" -> orgName.toLowerCase(Locale.ENGLISH),
        "1" -> orgDomain.toLowerCase(Locale.ENGLISH),
        "2" -> orgName.toUpperCase(Locale.ENGLISH),
        "3" -> orgDomain.toUpperCase(Locale.ENGLISH),
        "4" -> bindAddress
      )
      val reader = new BufferedReader(new InputStreamReader(MiniKdc.getResourceAsStream("minikdc.ldiff")))
      try {
        var line: String = null
        val builder = new StringBuilder
        while ({line = reader.readLine(); line != null})
          builder.append(line).append("\n")
        addEntriesToDirectoryService(StrSubstitutor.replace(builder, map.asJava))
      }
      finally CoreUtils.swallow(reader.close(), this)
    }

    val bindAddress = config.getProperty(MiniKdc.KdcBindAddress)
    addInitialEntriesToDirectoryService(bindAddress)

    val kerberosConfig = new KerberosConfig
    kerberosConfig.setMaximumRenewableLifetime(config.getProperty(MiniKdc.MaxRenewableLifetime).toLong)
    kerberosConfig.setMaximumTicketLifetime(config.getProperty(MiniKdc.MaxTicketLifetime).toLong)
    kerberosConfig.setSearchBaseDn(s"dc=$orgName,dc=$orgDomain")
    kerberosConfig.setPaEncTimestampRequired(false)
    kdc = new KdcServer(kerberosConfig)
    kdc.setDirectoryService(ds)

    // transport
    val transport = config.getProperty(MiniKdc.Transport)
    val absTransport = transport.trim match {
      case "TCP" => new TcpTransport(bindAddress, port, 3, 50)
      case "UDP" => new UdpTransport(port)
      case _ => throw new IllegalArgumentException(s"Invalid transport: $transport")
    }
    kdc.addTransports(absTransport)
    kdc.setServiceName(config.getProperty(MiniKdc.Instance))
    kdc.start()

    // if using ephemeral port, update port number for binding
    if (port == 0)
      _port = absTransport.getAcceptor.getLocalAddress.asInstanceOf[InetSocketAddress].getPort

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

        // Close kdc acceptors and wait for them to terminate, ensuring that sockets are closed before returning.
        for (transport <- kdc.getTransports) {
          val acceptor = transport.getAcceptor
          if (acceptor != null) acceptor.dispose(true)
        }
        kdc.stop()
        try ds.shutdown()
        catch {
          case ex: Exception => error("Could not shutdown ApacheDS properly", ex)
        }
      }
    }
  }

  /**
    * Creates a principal in the KDC with the specified user and password.
    *
    * An exception will be thrown if the principal cannot be created.
    *
    * @param principal principal name, do not include the domain.
    * @param password  password.
    */
  private def createPrincipal(principal: String, password: String): Unit = {
    val ldifContent = s"""
      |dn: uid=$principal,ou=users,dc=${orgName.toLowerCase(Locale.ENGLISH)},dc=${orgDomain.toLowerCase(Locale.ENGLISH)}
      |objectClass: top
      |objectClass: person
      |objectClass: inetOrgPerson
      |objectClass: krb5principal
      |objectClass: krb5kdcentry
      |cn: $principal
      |sn: $principal
      |uid: $principal
      |userPassword: $password
      |krb5PrincipalName: $principal@$realm
      |krb5KeyVersionNumber: 0""".stripMargin
    addEntriesToDirectoryService(ldifContent)
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
    val generatedPassword = UUID.randomUUID.toString
    val keytab = new Keytab
    val entries = principals.flatMap { principal =>
      createPrincipal(principal, generatedPassword)
      val principalWithRealm = s"$principal@$realm"
      val timestamp = new KerberosTime
      KerberosKeyFactory.getKerberosKeys(principalWithRealm, generatedPassword).asScala.values.map { encryptionKey =>
        val keyVersion = encryptionKey.getKeyVersion.toByte
        new KeytabEntry(principalWithRealm, 1, timestamp, keyVersion, encryptionKey)
      }
    }
    keytab.setEntries(entries.asJava)
    keytab.write(keytabFile)
  }

  private def addEntriesToDirectoryService(ldifContent: String): Unit = {
    val reader = new LdifReader(new StringReader(ldifContent))
    try {
      for (ldifEntry <- reader.asScala)
        ds.getAdminSession.add(new DefaultEntry(ds.getSchemaManager, ldifEntry.getEntry))
    } finally CoreUtils.swallow(reader.close(), this)
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
