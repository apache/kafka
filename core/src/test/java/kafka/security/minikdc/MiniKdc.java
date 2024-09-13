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
package kafka.security.minikdc;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.common.utils.Utils;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.schema.extractor.impl.DefaultSchemaLdifExtractor;
import org.apache.directory.api.ldap.schema.loader.LdifSchemaLoader;
import org.apache.directory.api.ldap.schema.manager.impl.DefaultSchemaManager;
import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.directory.server.core.DefaultDirectoryService;
import org.apache.directory.server.core.api.CacheService;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.api.InstanceLayout;
import org.apache.directory.server.core.api.schema.SchemaPartition;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmIndex;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmPartition;
import org.apache.directory.server.core.partition.ldif.LdifPartition;
import org.apache.directory.server.kerberos.KerberosConfig;
import org.apache.directory.server.kerberos.kdc.KdcServer;
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.directory.server.protocol.shared.transport.AbstractTransport;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.directory.server.protocol.shared.transport.Transport;
import org.apache.directory.server.protocol.shared.transport.UdpTransport;
import org.apache.directory.server.xdbm.Index;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.mina.core.service.IoAcceptor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
 */
@SuppressWarnings("ClassDataAbstractionCoupling")
public class MiniKdc {
    static final String ORG_NAME = "org.name";
    static final String ORG_DOMAIN = "org.domain";
    static final String KDC_BIND_ADDRESS = "kdc.bind.address";
    static final String KDC_PORT = "kdc.port";
    static final String INSTANCE = "instance";
    static final String MAX_TICKET_LIFETIME = "max.ticket.lifetime";
    static final String MAX_RENEWABLE_LIFETIME = "max.renewable.lifetime";
    static final String TRANSPORT = "transport";
    static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    private static final String SUN_SECURITY_KRB5_DEBUG = "sun.security.krb5.debug";
    private static final String DEBUG = "debug";

    private final String orgName;
    private final String orgDomain;
    private final String realm;
    private final String host;
    private int port;

    private final Properties config;
    private final File workDir;
    private final File krb5conf;

    private DirectoryService ds;
    private KdcServer kdc;
    private boolean closed = false;

    /**
     * @param config the MiniKdc configuration
     * @param workDir the working directory which will contain krb5.conf, Apache DS files and any other files needed by
     *                MiniKdc.
     */
    public MiniKdc(Properties config, File workDir) {
        Set<String> requiredProperties = new HashSet<>(Arrays.asList(ORG_NAME, ORG_DOMAIN, KDC_BIND_ADDRESS, KDC_PORT,
                INSTANCE, TRANSPORT, MAX_TICKET_LIFETIME, MAX_RENEWABLE_LIFETIME));
        if (!config.keySet().containsAll(requiredProperties)) {
            throw new IllegalArgumentException("Missing required properties: " + requiredProperties);
        }
        this.config = config;
        this.workDir = workDir;

        this.orgName = config.getProperty(ORG_NAME);
        this.orgDomain = config.getProperty(ORG_DOMAIN);
        this.realm = this.orgName.toUpperCase(Locale.ENGLISH) + "." + this.orgDomain.toUpperCase(Locale.ENGLISH);
        this.krb5conf = new File(workDir, "krb5.conf");
        this.host = config.getProperty(KDC_BIND_ADDRESS);
        this.port = Integer.parseInt(config.getProperty(KDC_PORT));
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Arguments: <WORKDIR> <MINIKDCPROPERTIES> <KEYTABFILE> [<PRINCIPALS>]+");
            Exit.exit(1);
        }

        String workDirPath = args[0];
        String configPath = args[1];
        String keytabPath = args[2];
        String[] principals = new String[args.length - 3];
        System.arraycopy(args, 3, principals, 0, principals.length);

        File workDir = new File(workDirPath);
        if (!workDir.exists()) {
            throw new RuntimeException("Specified work directory does not exist: " + workDir.getAbsolutePath());
        }

        Properties config = createConfig();
        File configFile = new File(configPath);
        if (!configFile.exists()) {
            throw new RuntimeException("Specified configuration does not exist: " + configFile.getAbsolutePath());
        }

        Properties userConfig = Utils.loadProps(configFile.getAbsolutePath());
        config.putAll(userConfig);
        File keytabFile = new File(keytabPath).getAbsoluteFile();

        start(workDir, config, keytabFile, Arrays.asList(principals));
    }

    /**
     * Convenience method that returns MiniKdc default configuration.
     *
     * The returned configuration is a copy, it can be customized before using
     * it to create a MiniKdc.
     */
    public static Properties createConfig() {
        Properties properties = new Properties();
        properties.put(ORG_NAME, "EXAMPLE");
        properties.put(ORG_DOMAIN, "COM");
        properties.put(KDC_BIND_ADDRESS, "localhost");
        properties.put(KDC_PORT, "0");
        properties.put(INSTANCE, "DefaultKrbServer");
        properties.put(MAX_TICKET_LIFETIME, "86400000");
        properties.put(MAX_RENEWABLE_LIFETIME, "604800000");
        properties.put(TRANSPORT, "TCP");
        properties.put(DEBUG, "false");
        return properties;
    }


    public void start() throws Exception {
        if (kdc != null) {
            throw new IllegalStateException("KDC already started");
        }
        if (closed) {
            throw new IllegalStateException("KDC already closed");
        }

        initDirectoryService();
        initKdcServer();
        initJvmKerberosConfig();
    }

    public static MiniKdc start(File workDir, Properties config, File keytabFile, List<String> principals) throws Exception {
        MiniKdc miniKdc = new MiniKdc(config, workDir);
        miniKdc.start();
        miniKdc.createPrincipal(keytabFile, principals);
        String infoMessage = String.format(
                "\n" +
                        "Standalone MiniKdc Running\n" +
                        "---------------------------------------------------\n" +
                        "  Realm           : %s\n" +
                        "  Running at      : %s:%d\n" +
                        "  krb5conf        : %s\n" +
                        "\n" +
                        "  created keytab  : %s\n" +
                        "  with principals : %s\n" +
                        "\n" +
                        "Hit <CTRL-C> or kill <PID> to stop it\n" +
                        "---------------------------------------------------\n",
                miniKdc.getRealm(), miniKdc.getHost(), miniKdc.getPort(), miniKdc.getKrb5conf().getAbsolutePath(),
                keytabFile.getAbsolutePath(), String.join(", ", principals)
        );
        System.out.println(infoMessage);
        Exit.addShutdownHook("minikdc-shutdown-hook", miniKdc::stop);
        return miniKdc;
    }

    public String getRealm() {
        return realm;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public File getKrb5conf() {
        return krb5conf;
    }

    public void stop() {
        if (!closed) {
            closed = true;
            if (kdc != null) {
                System.clearProperty(JAVA_SECURITY_KRB5_CONF);
                System.clearProperty(SUN_SECURITY_KRB5_DEBUG);

                // Close kdc acceptors and wait for them to terminate, ensuring that sockets are closed before returning.
                for (Transport transport: kdc.getTransports()) {
                    IoAcceptor acceptor = transport.getAcceptor();
                    if (acceptor != null) acceptor.dispose(true);
                }
                kdc.stop();
                try {
                    ds.shutdown();
                } catch (Exception ex) {
                    System.err.println("Could not shutdown ApacheDS properly, exception: " + ex);
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
    public void createPrincipal(File keytabFile, List<String> principals) throws IOException {
        String generatedPassword = UUID.randomUUID().toString();
        Keytab keytab = new Keytab();
        List<KeytabEntry> entries = principals.stream().flatMap(principal -> {
            try {
                createPrincipal(principal, generatedPassword);
            } catch (LdapException | IOException e) {
                throw new RuntimeException(e);
            }
            String principalWithRealm = principal + "@" + realm;
            KerberosTime timestamp = new KerberosTime();
            return KerberosKeyFactory.getKerberosKeys(principalWithRealm, generatedPassword).values().stream().map(encryptionKey -> {
                byte keyVersion = (byte) encryptionKey.getKeyVersion();
                return new KeytabEntry(principalWithRealm, 1, timestamp, keyVersion, encryptionKey);
            });
        }).collect(Collectors.toList());
        keytab.setEntries(entries);
        keytab.write(keytabFile);
    }

    private void initDirectoryService() throws Exception {
        ds = new DefaultDirectoryService();
        ds.setInstanceLayout(new InstanceLayout(workDir));
        ds.setCacheService(new CacheService());

        // first load the schema
        InstanceLayout instanceLayout = ds.getInstanceLayout();
        File schemaPartitionDirectory = new File(instanceLayout.getPartitionsDirectory(), "schema");
        DefaultSchemaLdifExtractor extractor = new DefaultSchemaLdifExtractor(instanceLayout.getPartitionsDirectory());
        extractor.extractOrCopy();

        LdifSchemaLoader loader = new LdifSchemaLoader(schemaPartitionDirectory);
        DefaultSchemaManager schemaManager = new DefaultSchemaManager(loader);
        schemaManager.loadAllEnabled();
        ds.setSchemaManager(schemaManager);

        // Init the LdifPartition with schema
        LdifPartition schemaLdifPartition = new LdifPartition(schemaManager, ds.getDnFactory());
        schemaLdifPartition.setPartitionPath(schemaPartitionDirectory.toURI());

        // The schema partition
        SchemaPartition schemaPartition = new SchemaPartition(schemaManager);
        schemaPartition.setWrappedPartition(schemaLdifPartition);
        ds.setSchemaPartition(schemaPartition);

        JdbmPartition systemPartition = new JdbmPartition(ds.getSchemaManager(), ds.getDnFactory());
        systemPartition.setId("system");
        systemPartition.setPartitionPath(new File(ds.getInstanceLayout().getPartitionsDirectory(), systemPartition.getId()).toURI());
        systemPartition.setSuffixDn(new Dn(ServerDNConstants.SYSTEM_DN));
        systemPartition.setSchemaManager(ds.getSchemaManager());
        ds.setSystemPartition(systemPartition);

        ds.getChangeLog().setEnabled(false);
        ds.setDenormalizeOpAttrsEnabled(true);
        ds.addLast(new KeyDerivationInterceptor());

        // create one partition
        String orgName = config.getProperty(ORG_NAME).toLowerCase(Locale.ENGLISH);
        String orgDomain = config.getProperty(ORG_DOMAIN).toLowerCase(Locale.ENGLISH);
        JdbmPartition partition = new JdbmPartition(ds.getSchemaManager(), ds.getDnFactory());
        partition.setId(orgName);
        partition.setPartitionPath(new File(ds.getInstanceLayout().getPartitionsDirectory(), orgName).toURI());
        Dn dn = new Dn("dc=" + orgName + ",dc=" + orgDomain);
        partition.setSuffixDn(dn);
        ds.addPartition(partition);

        // indexes
        Set<Index<?, String>> indexedAttributes = new HashSet<>();
        indexedAttributes.add(new JdbmIndex<>("objectClass", false));
        indexedAttributes.add(new JdbmIndex<>("dc", false));
        indexedAttributes.add(new JdbmIndex<>("ou", false));
        partition.setIndexedAttributes(indexedAttributes);

        // And start the ds
        ds.setInstanceId(config.getProperty(INSTANCE));
        ds.setShutdownHookEnabled(false);
        ds.startup();

        // context entry, after ds.startup()
        Entry entry = ds.newEntry(dn);
        entry.add("objectClass", "top", "domain");
        entry.add("dc", orgName);
        ds.getAdminSession().add(entry);
    }

    private void addInitialEntriesToDirectoryService(String bindAddress) throws IOException {
        Map<String, String> map = new HashMap<>();
        map.put("0", orgName.toLowerCase(Locale.ENGLISH));
        map.put("1", orgDomain.toLowerCase(Locale.ENGLISH));
        map.put("2", orgName.toUpperCase(Locale.ENGLISH));
        map.put("3", orgDomain.toUpperCase(Locale.ENGLISH));
        map.put("4", bindAddress);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(getResourceAsStream("minikdc.ldiff")))) {
            StringBuilder builder = new StringBuilder();
            reader.lines().forEach(line -> builder.append(line).append("\n"));
            addEntriesToDirectoryService(StrSubstitutor.replace(builder, map));
        } catch (LdapException e) {
            throw new RuntimeException(e);
        }
    }

    private void initKdcServer() throws IOException, LdapInvalidDnException {
        String bindAddress = config.getProperty(KDC_BIND_ADDRESS);
        addInitialEntriesToDirectoryService(bindAddress);

        KerberosConfig kerberosConfig = new KerberosConfig();
        kerberosConfig.setMaximumRenewableLifetime(Long.parseLong(config.getProperty(MAX_RENEWABLE_LIFETIME)));
        kerberosConfig.setMaximumTicketLifetime(Long.parseLong(config.getProperty(MAX_TICKET_LIFETIME)));
        kerberosConfig.setSearchBaseDn("dc=" + orgName + ",dc=" + orgDomain);
        kerberosConfig.setPaEncTimestampRequired(false);
        kdc = new KdcServer(kerberosConfig);
        kdc.setDirectoryService(ds);

        // transport
        AbstractTransport absTransport;
        String transport = config.getProperty(TRANSPORT).trim();
        switch (transport) {
            case "TCP":
                absTransport = new TcpTransport(bindAddress, port, 3, 50);
                break;
            case "UDP":
                absTransport = new UdpTransport(port);
                break;
            default:
                throw new IllegalArgumentException("Invalid transport: " + transport);
        }
        kdc.addTransports(absTransport);
        kdc.setServiceName(config.getProperty(INSTANCE));
        kdc.start();

        // if using ephemeral port, update port number for binding
        if (port == 0) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) absTransport.getAcceptor().getLocalAddress();
            port = inetSocketAddress.getPort();
        }

        System.out.println("MiniKdc listening at port: " + port);
    }

    private void initJvmKerberosConfig() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        writeKrb5Conf();
        System.setProperty(JAVA_SECURITY_KRB5_CONF, krb5conf.getAbsolutePath());
        System.setProperty(SUN_SECURITY_KRB5_DEBUG, config.getProperty(DEBUG, "false"));
        System.out.println("MiniKdc setting JVM krb5.conf to: " + krb5conf.getAbsolutePath());
        refreshJvmKerberosConfig();
    }

    private void writeKrb5Conf() throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(getResourceAsStream("minikdc-krb5.conf"), StandardCharsets.UTF_8))) {
            reader.lines().forEach(line -> stringBuilder.append(line).append("{3}"));
        }
        String output = MessageFormat.format(stringBuilder.toString(), realm, host, String.valueOf(port), System.lineSeparator());
        Files.write(krb5conf.toPath(), output.getBytes(StandardCharsets.UTF_8));
    }

    private void refreshJvmKerberosConfig() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> klass;
        if (Java.isIbmJdk() && !Java.isIbmJdkSemeru())
            klass = Class.forName("com.ibm.security.krb5.internal.Config");
        else
            klass = Class.forName("sun.security.krb5.Config");
        klass.getMethod("refresh").invoke(klass);
    }

    private InputStream getResourceAsStream(String resourceName) throws IOException {
        ClassLoader cl = Optional.of(Thread.currentThread().getContextClassLoader())
                .orElse(MiniKdc.class.getClassLoader());
        InputStream resourceStream = cl.getResourceAsStream(resourceName);
        if (resourceStream == null) {
            throw new IOException("Can not read resource file " + resourceName);
        }
        return resourceStream;
    }

    /**
     * Creates a principal in the KDC with the specified user and password.
     *
     * An exception will be thrown if the principal cannot be created.
     *
     * @param principal principal name, do not include the domain.
     * @param password  password.
     */
    private void createPrincipal(String principal, String password) throws LdapException, IOException {
        String ldifContent = "dn: uid=" + principal + ",ou=users,dc=" + orgName + ",dc=" + orgDomain + "\n" +
                "objectClass: top\n" +
                "objectClass: person\n" +
                "objectClass: organizationalPerson\n" +
                "objectClass: inetOrgPerson\n" +
                "objectClass: krb5principal\n" +
                "objectClass: krb5kdcentry\n" +
                "cn: " + principal + "\n" +
                "sn: " + principal + "\n" +
                "uid: " + principal + "\n" +
                "userPassword: " + password + "\n" +
                "krb5PrincipalName: " + principal + "@" + realm + "\n" +
                "krb5KeyVersionNumber: 0\n";

        addEntriesToDirectoryService(ldifContent);
    }

    private void addEntriesToDirectoryService(String ldifContent) throws LdapException, IOException {
        try (LdifReader reader = new LdifReader(new StringReader(ldifContent))) {
            reader.forEach(ldifEntry -> {
                try {
                    ds.getAdminSession().add(new DefaultEntry(ds.getSchemaManager(), ldifEntry.getEntry()));
                } catch (LdapException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
