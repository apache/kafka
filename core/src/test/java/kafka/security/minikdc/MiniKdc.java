/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.security.minikdc;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.directory.api.ldap.model.schema.SchemaManager;
import org.apache.directory.api.ldap.schemaextractor.SchemaLdifExtractor;
import org.apache.directory.api.ldap.schemaextractor.impl.DefaultSchemaLdifExtractor;
import org.apache.directory.api.ldap.schemaloader.LdifSchemaLoader;
import org.apache.directory.api.ldap.schemamanager.impl.DefaultSchemaManager;
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
import org.apache.directory.server.protocol.shared.transport.UdpTransport;
import org.apache.directory.server.xdbm.Index;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.directory.shared.kerberos.components.EncryptionKey;
import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.schema.registries.SchemaLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * NOTE: This is a copy of org.apache.hadoop.minikdc.MiniKdc from the following revision:
 *
 * https://github.com/apache/hadoop/blob/8fb70a031b323634ddc51ff6aff4f376baef68c8/hadoop-common-project/hadoop-minikdc/src/main/java/org/apache/hadoop/minikdc/MiniKdc.java
 *
 * This is a temporary measure to improve stability of our tests (KAFKA-3453) until MiniKdc 2.8.0 is released.
 * Once that happens, we should upgrade MiniKdc, remove this class and revert changes that were done in the
 * commit where this class was added.
 *
 * Mini KDC based on Apache Directory Server that can be embedded in testcases
 * or used from command line as a standalone KDC.
 * <p>
 * <b>From within testcases:</b>
 * <p>
 * MiniKdc sets 2 System properties when started and un-sets them when stopped:
 * <ul>
 *   <li>java.security.krb5.conf: set to the MiniKDC real/host/port</li>
 *   <li>sun.security.krb5.debug: set to the debug value provided in the
 *   configuration</li>
 * </ul>
 * Because of this, multiple MiniKdc instances cannot be started in parallel.
 * For example, running testcases in parallel that start a KDC each. To
 * accomplish this a single MiniKdc should be used for all testcases running
 * in parallel.
 * <p>
 * MiniKdc default configuration values are:
 * <ul>
 *   <li>org.name=EXAMPLE (used to create the REALM)</li>
 *   <li>org.domain=COM (used to create the REALM)</li>
 *   <li>kdc.bind.address=localhost</li>
 *   <li>kdc.port=0 (ephemeral port)</li>
 *   <li>instance=DefaultKrbServer</li>
 *   <li>max.ticket.lifetime=86400000 (1 day)</li>
 *   <li>max.renewable.lifetime=604800000 (7 days)</li>
 *   <li>transport=TCP</li>
 *   <li>debug=false</li>
 * </ul>
 * The generated krb5.conf forces TCP connections.
 */
public class MiniKdc {

    public static final String JAVA_SECURITY_KRB5_CONF =
            "java.security.krb5.conf";
    public static final String SUN_SECURITY_KRB5_DEBUG =
            "sun.security.krb5.debug";

    public static void main(String[] args) throws  Exception {
        if (args.length < 4) {
            System.out.println("Arguments: <WORKDIR> <MINIKDCPROPERTIES> " +
                    "<KEYTABFILE> [<PRINCIPALS>]+");
            System.exit(1);
        }
        File workDir = new File(args[0]);
        if (!workDir.exists()) {
            throw new RuntimeException("Specified work directory does not exists: "
                    + workDir.getAbsolutePath());
        }
        Properties conf = createConf();
        File file = new File(args[1]);
        if (!file.exists()) {
            throw new RuntimeException("Specified configuration does not exists: "
                    + file.getAbsolutePath());
        }
        Properties userConf = new Properties();
        InputStreamReader r = null;
        try {
            r = new InputStreamReader(new FileInputStream(file), Charsets.UTF_8);
            userConf.load(r);
        } finally {
            if (r != null) {
                r.close();
            }
        }
        for (Map.Entry<?, ?> entry : userConf.entrySet()) {
            conf.put(entry.getKey(), entry.getValue());
        }
        final MiniKdc miniKdc = new MiniKdc(conf, workDir);
        miniKdc.start();
        File krb5conf = new File(workDir, "krb5.conf");
        if (miniKdc.getKrb5conf().renameTo(krb5conf)) {
            File keytabFile = new File(args[2]).getAbsoluteFile();
            String[] principals = new String[args.length - 3];
            System.arraycopy(args, 3, principals, 0, args.length - 3);
            miniKdc.createPrincipal(keytabFile, principals);
            System.out.println();
            System.out.println("Standalone MiniKdc Running");
            System.out.println("---------------------------------------------------");
            System.out.println("  Realm           : " + miniKdc.getRealm());
            System.out.println("  Running at      : " + miniKdc.getHost() + ":" +
                    miniKdc.getHost());
            System.out.println("  krb5conf        : " + krb5conf);
            System.out.println();
            System.out.println("  created keytab  : " + keytabFile);
            System.out.println("  with principals : " + Arrays.asList(principals));
            System.out.println();
            System.out.println(" Do <CTRL-C> or kill <PID> to stop it");
            System.out.println("---------------------------------------------------");
            System.out.println();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    miniKdc.stop();
                }
            });
        } else {
            throw new RuntimeException("Cannot rename KDC's krb5conf to "
                    + krb5conf.getAbsolutePath());
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(MiniKdc.class);

    public static final String ORG_NAME = "org.name";
    public static final String ORG_DOMAIN = "org.domain";
    public static final String KDC_BIND_ADDRESS = "kdc.bind.address";
    public static final String KDC_PORT = "kdc.port";
    public static final String INSTANCE = "instance";
    public static final String MAX_TICKET_LIFETIME = "max.ticket.lifetime";
    public static final String MAX_RENEWABLE_LIFETIME = "max.renewable.lifetime";
    public static final String TRANSPORT = "transport";
    public static final String DEBUG = "debug";

    private static final Set<String> PROPERTIES = new HashSet<String>();
    private static final Properties DEFAULT_CONFIG = new Properties();

    static {
        PROPERTIES.add(ORG_NAME);
        PROPERTIES.add(ORG_DOMAIN);
        PROPERTIES.add(KDC_BIND_ADDRESS);
        PROPERTIES.add(KDC_BIND_ADDRESS);
        PROPERTIES.add(KDC_PORT);
        PROPERTIES.add(INSTANCE);
        PROPERTIES.add(TRANSPORT);
        PROPERTIES.add(MAX_TICKET_LIFETIME);
        PROPERTIES.add(MAX_RENEWABLE_LIFETIME);

        DEFAULT_CONFIG.setProperty(KDC_BIND_ADDRESS, "localhost");
        DEFAULT_CONFIG.setProperty(KDC_PORT, "0");
        DEFAULT_CONFIG.setProperty(INSTANCE, "DefaultKrbServer");
        DEFAULT_CONFIG.setProperty(ORG_NAME, "EXAMPLE");
        DEFAULT_CONFIG.setProperty(ORG_DOMAIN, "COM");
        DEFAULT_CONFIG.setProperty(TRANSPORT, "TCP");
        DEFAULT_CONFIG.setProperty(MAX_TICKET_LIFETIME, "86400000");
        DEFAULT_CONFIG.setProperty(MAX_RENEWABLE_LIFETIME, "604800000");
        DEFAULT_CONFIG.setProperty(DEBUG, "false");
    }

    /**
     * Convenience method that returns MiniKdc default configuration.
     * <p>
     * The returned configuration is a copy, it can be customized before using
     * it to create a MiniKdc.
     * @return a MiniKdc default configuration.
     */
    public static Properties createConf() {
        return (Properties) DEFAULT_CONFIG.clone();
    }

    private Properties conf;
    private DirectoryService ds;
    private KdcServer kdc;
    private int port;
    private String realm;
    private File workDir;
    private File krb5conf;

    /**
     * Creates a MiniKdc.
     *
     * @param conf MiniKdc configuration.
     * @param workDir working directory, it should be the build directory. Under
     * this directory an ApacheDS working directory will be created, this
     * directory will be deleted when the MiniKdc stops.
     * @throws Exception thrown if the MiniKdc could not be created.
     */
    public MiniKdc(Properties conf, File workDir) throws Exception {
        if (!conf.keySet().containsAll(PROPERTIES)) {
            Set<String> missingProperties = new HashSet<String>(PROPERTIES);
            missingProperties.removeAll(conf.keySet());
            throw new IllegalArgumentException("Missing configuration properties: "
                    + missingProperties);
        }
        this.workDir = new File(workDir, Long.toString(System.currentTimeMillis()));
        if (!workDir.exists()
                && !workDir.mkdirs()) {
            throw new RuntimeException("Cannot create directory " + workDir);
        }
        LOG.info("Configuration:");
        LOG.info("---------------------------------------------------------------");
        for (Map.Entry<?, ?> entry : conf.entrySet()) {
            LOG.info("  {}: {}", entry.getKey(), entry.getValue());
        }
        LOG.info("---------------------------------------------------------------");
        this.conf = conf;
        port = Integer.parseInt(conf.getProperty(KDC_PORT));
        String orgName = conf.getProperty(ORG_NAME);
        String orgDomain = conf.getProperty(ORG_DOMAIN);
        realm = orgName.toUpperCase(Locale.ENGLISH) + "."
                + orgDomain.toUpperCase(Locale.ENGLISH);
    }

    /**
     * Returns the port of the MiniKdc.
     *
     * @return the port of the MiniKdc.
     */
    public int getPort() {
        return port;
    }

    /**
     * Returns the host of the MiniKdc.
     *
     * @return the host of the MiniKdc.
     */
    public String getHost() {
        return conf.getProperty(KDC_BIND_ADDRESS);
    }

    /**
     * Returns the realm of the MiniKdc.
     *
     * @return the realm of the MiniKdc.
     */
    public String getRealm() {
        return realm;
    }

    public File getKrb5conf() {
        return krb5conf;
    }

    /**
     * Starts the MiniKdc.
     *
     * @throws Exception thrown if the MiniKdc could not be started.
     */
    public synchronized void start() throws Exception {
        if (kdc != null) {
            throw new RuntimeException("Already started");
        }
        initDirectoryService();
        initKDCServer();
    }

    private void initDirectoryService() throws Exception {
        ds = new DefaultDirectoryService();
        ds.setInstanceLayout(new InstanceLayout(workDir));

        CacheService cacheService = new CacheService();
        ds.setCacheService(cacheService);

        // first load the schema
        InstanceLayout instanceLayout = ds.getInstanceLayout();
        File schemaPartitionDirectory = new File(
                instanceLayout.getPartitionsDirectory(), "schema");
        SchemaLdifExtractor extractor = new DefaultSchemaLdifExtractor(
                instanceLayout.getPartitionsDirectory());
        extractor.extractOrCopy();

        SchemaLoader loader = new LdifSchemaLoader(schemaPartitionDirectory);
        SchemaManager schemaManager = new DefaultSchemaManager(loader);
        schemaManager.loadAllEnabled();
        ds.setSchemaManager(schemaManager);
        // Init the LdifPartition with schema
        LdifPartition schemaLdifPartition = new LdifPartition(schemaManager);
        schemaLdifPartition.setPartitionPath(schemaPartitionDirectory.toURI());

        // The schema partition
        SchemaPartition schemaPartition = new SchemaPartition(schemaManager);
        schemaPartition.setWrappedPartition(schemaLdifPartition);
        ds.setSchemaPartition(schemaPartition);

        JdbmPartition systemPartition = new JdbmPartition(ds.getSchemaManager());
        systemPartition.setId("system");
        systemPartition.setPartitionPath(new File(
                ds.getInstanceLayout().getPartitionsDirectory(),
                systemPartition.getId()).toURI());
        systemPartition.setSuffixDn(new Dn(ServerDNConstants.SYSTEM_DN));
        systemPartition.setSchemaManager(ds.getSchemaManager());
        ds.setSystemPartition(systemPartition);

        ds.getChangeLog().setEnabled(false);
        ds.setDenormalizeOpAttrsEnabled(true);
        ds.addLast(new KeyDerivationInterceptor());

        // create one partition
        String orgName = conf.getProperty(ORG_NAME).toLowerCase(Locale.ENGLISH);
        String orgDomain = conf.getProperty(ORG_DOMAIN).toLowerCase(Locale.ENGLISH);

        JdbmPartition partition = new JdbmPartition(ds.getSchemaManager());
        partition.setId(orgName);
        partition.setPartitionPath(new File(
                ds.getInstanceLayout().getPartitionsDirectory(), orgName).toURI());
        partition.setSuffixDn(new Dn("dc=" + orgName + ",dc=" + orgDomain));
        ds.addPartition(partition);
        // indexes
        Set<Index<?, ?, String>> indexedAttributes = new HashSet<Index<?, ?, String>>();
        indexedAttributes.add(new JdbmIndex<String, Entry>("objectClass", false));
        indexedAttributes.add(new JdbmIndex<String, Entry>("dc", false));
        indexedAttributes.add(new JdbmIndex<String, Entry>("ou", false));
        partition.setIndexedAttributes(indexedAttributes);

        // And start the ds
        ds.setInstanceId(conf.getProperty(INSTANCE));
        ds.startup();
        // context entry, after ds.startup()
        Dn dn = new Dn("dc=" + orgName + ",dc=" + orgDomain);
        Entry entry = ds.newEntry(dn);
        entry.add("objectClass", "top", "domain");
        entry.add("dc", orgName);
        ds.getAdminSession().add(entry);
    }

    /**
     * Convenience method that returns a resource as inputstream from the
     * classpath.
     * <p>
     * It first attempts to use the Thread's context classloader and if not
     * set it uses the class' classloader.
     *
     * @param resourceName resource to retrieve.
     *
     * @throws IOException thrown if resource cannot be loaded
     * @return inputstream with the resource.
     */
    public static InputStream getResourceAsStream(String resourceName)
            throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = MiniKdc.class.getClassLoader();
        }
        InputStream is = cl.getResourceAsStream(resourceName);
        if (is == null) {
            throw new IOException("Can not read resource file '" +
                    resourceName + "'");
        }
        return is;
    }

    private void initKDCServer() throws Exception {
        String orgName = conf.getProperty(ORG_NAME);
        String orgDomain = conf.getProperty(ORG_DOMAIN);
        String bindAddress = conf.getProperty(KDC_BIND_ADDRESS);
        final Map<String, String> map = new HashMap<String, String>();
        map.put("0", orgName.toLowerCase(Locale.ENGLISH));
        map.put("1", orgDomain.toLowerCase(Locale.ENGLISH));
        map.put("2", orgName.toUpperCase(Locale.ENGLISH));
        map.put("3", orgDomain.toUpperCase(Locale.ENGLISH));
        map.put("4", bindAddress);

        InputStream is1 = getResourceAsStream("minikdc.ldiff");

        SchemaManager schemaManager = ds.getSchemaManager();
        LdifReader reader = null;

        try {
            final String content = StrSubstitutor.replace(IOUtils.toString(is1), map);
            reader = new LdifReader(new StringReader(content));

            for (LdifEntry ldifEntry : reader) {
                ds.getAdminSession().add(new DefaultEntry(schemaManager,
                        ldifEntry.getEntry()));
            }
        } finally {
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(is1);
        }

        KerberosConfig kerberosConfig = new KerberosConfig();
        kerberosConfig.setMaximumRenewableLifetime(Long.parseLong(conf
                .getProperty(MAX_RENEWABLE_LIFETIME)));
        kerberosConfig.setMaximumTicketLifetime(Long.parseLong(conf
                .getProperty(MAX_TICKET_LIFETIME)));
        kerberosConfig.setSearchBaseDn(String.format("dc=%s,dc=%s", orgName,
                orgDomain));
        kerberosConfig.setPaEncTimestampRequired(false);
        kdc = new KdcServer(kerberosConfig);
        kdc.setDirectoryService(ds);

        // transport
        String transport = conf.getProperty(TRANSPORT);
        AbstractTransport absTransport;
        if (transport.trim().equals("TCP")) {
            absTransport = new TcpTransport(bindAddress, port, 3, 50);
        } else if (transport.trim().equals("UDP")) {
            absTransport = new UdpTransport(port);
        } else {
            throw new IllegalArgumentException("Invalid transport: " + transport);
        }
        kdc.addTransports(absTransport);
        kdc.setServiceName(conf.getProperty(INSTANCE));
        kdc.start();
        // if using ephemeral port, update port number for binding
        if (port == 0) {
            InetSocketAddress addr =
                    (InetSocketAddress) absTransport.getAcceptor().getLocalAddress();
            port = addr.getPort();
        }

        StringBuilder sb = new StringBuilder();
        InputStream is2 = getResourceAsStream("minikdc-krb5.conf");

        BufferedReader r = null;

        try {
            r = new BufferedReader(new InputStreamReader(is2, Charsets.UTF_8));
            String line = r.readLine();

            while (line != null) {
                sb.append(line).append("{3}");
                line = r.readLine();
            }
        } finally {
            IOUtils.closeQuietly(r);
            IOUtils.closeQuietly(is2);
        }

        krb5conf = new File(workDir, "krb5.conf").getAbsoluteFile();
        FileUtils.writeStringToFile(krb5conf,
                MessageFormat.format(sb.toString(), getRealm(), getHost(),
                        Integer.toString(getPort()), System.getProperty("line.separator")));
        System.setProperty(JAVA_SECURITY_KRB5_CONF, krb5conf.getAbsolutePath());

        System.setProperty(SUN_SECURITY_KRB5_DEBUG, conf.getProperty(DEBUG,
                "false"));

        // refresh the config
        Class<?> classRef;
        if (System.getProperty("java.vendor").contains("IBM")) {
            classRef = Class.forName("com.ibm.security.krb5.internal.Config");
        } else {
            classRef = Class.forName("sun.security.krb5.Config");
        }
        Method refreshMethod = classRef.getMethod("refresh", new Class[0]);
        refreshMethod.invoke(classRef, new Object[0]);

        LOG.info("MiniKdc listening at port: {}", getPort());
        LOG.info("MiniKdc setting JVM krb5.conf to: {}",
                krb5conf.getAbsolutePath());
    }

    /**
     * Stops the MiniKdc
     */
    public synchronized void stop() {
        if (kdc != null) {
            System.getProperties().remove(JAVA_SECURITY_KRB5_CONF);
            System.getProperties().remove(SUN_SECURITY_KRB5_DEBUG);
            kdc.stop();
            try {
                ds.shutdown();
            } catch (Exception ex) {
                LOG.error("Could not shutdown ApacheDS properly: {}", ex.toString(),
                        ex);
            }
        }
        delete(workDir);
    }

    private void delete(File f) {
        if (f.isFile()) {
            if (!f.delete()) {
                LOG.warn("WARNING: cannot delete file " + f.getAbsolutePath());
            }
        } else {
            for (File c: f.listFiles()) {
                delete(c);
            }
            if (!f.delete()) {
                LOG.warn("WARNING: cannot delete directory " + f.getAbsolutePath());
            }
        }
    }

    /**
     * Creates a principal in the KDC with the specified user and password.
     *
     * @param principal principal name, do not include the domain.
     * @param password password.
     * @throws Exception thrown if the principal could not be created.
     */
    public synchronized void createPrincipal(String principal, String password)
            throws Exception {
        String orgName = conf.getProperty(ORG_NAME);
        String orgDomain = conf.getProperty(ORG_DOMAIN);
        String baseDn = "ou=users,dc=" + orgName.toLowerCase(Locale.ENGLISH)
                + ",dc=" + orgDomain.toLowerCase(Locale.ENGLISH);
        String content = "dn: uid=" + principal + "," + baseDn + "\n" +
                "objectClass: top\n" +
                "objectClass: person\n" +
                "objectClass: inetOrgPerson\n" +
                "objectClass: krb5principal\n" +
                "objectClass: krb5kdcentry\n" +
                "cn: " + principal + "\n" +
                "sn: " + principal + "\n" +
                "uid: " + principal + "\n" +
                "userPassword: " + password + "\n" +
                "krb5PrincipalName: " + principal + "@" + getRealm() + "\n" +
                "krb5KeyVersionNumber: 0";

        for (LdifEntry ldifEntry : new LdifReader(new StringReader(content))) {
            ds.getAdminSession().add(new DefaultEntry(ds.getSchemaManager(),
                    ldifEntry.getEntry()));
        }
    }

    /**
     * Creates  multiple principals in the KDC and adds them to a keytab file.
     *
     * @param keytabFile keytab file to add the created principal.s
     * @param principals principals to add to the KDC, do not include the domain.
     * @throws Exception thrown if the principals or the keytab file could not be
     * created.
     */
    public void createPrincipal(File keytabFile, String ... principals)
            throws Exception {
        String generatedPassword = UUID.randomUUID().toString();
        Keytab keytab = new Keytab();
        List<KeytabEntry> entries = new ArrayList<KeytabEntry>();
        for (String principal : principals) {
            createPrincipal(principal, generatedPassword);
            principal = principal + "@" + getRealm();
            KerberosTime timestamp = new KerberosTime();
            for (Map.Entry<EncryptionType, EncryptionKey> entry : KerberosKeyFactory
                    .getKerberosKeys(principal, generatedPassword).entrySet()) {
                EncryptionKey ekey = entry.getValue();
                byte keyVersion = (byte) ekey.getKeyVersion();
                entries.add(new KeytabEntry(principal, 1L, timestamp, keyVersion,
                        ekey));
            }
        }
        keytab.setEntries(entries);
        keytab.write(keytabFile);
    }
}
