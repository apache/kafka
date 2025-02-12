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
package org.apache.kafka.tools;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import joptsimple.OptionSpec;

/**
 * A program for reading JMX metrics from a given endpoint.
 * <p>
 * This tool only works reliably if the JmxServer is fully initialized prior to invoking the tool.
 * See KAFKA-4620 for details.
 */
public class JmxTool {
    public static void main(String[] args) {
        try {
            JmxToolOptions options = new JmxToolOptions(args);
            if (CommandLineUtils.isPrintHelpNeeded(options)) {
                CommandLineUtils.printUsageAndExit(options.parser, "Dump JMX values to standard output.");
                return;
            }
            if (CommandLineUtils.isPrintVersionNeeded(options)) {
                CommandLineUtils.printVersionAndExit();
                return;
            }

            Optional<String[]> attributesInclude = options.attributesInclude();
            Optional<DateFormat> dateFormat = options.dateFormat();
            String reportFormat = options.parseFormat();
            boolean keepGoing = true;

            MBeanServerConnection conn = connectToBeanServer(options);
            List<ObjectName> queries = options.queries();
            boolean hasPatternQueries = queries.stream().filter(Objects::nonNull).anyMatch(ObjectName::isPattern);

            Set<ObjectName> found = findObjects(options, conn, queries, hasPatternQueries);
            Map<ObjectName, Integer> numExpectedAttributes =
                    findNumExpectedAttributes(conn, attributesInclude, hasPatternQueries, queries, found);

            List<String> keys = new ArrayList<>();
            keys.add("time");
            keys.addAll(new TreeSet<>(queryAttributes(conn, found, attributesInclude).keySet()));
            maybePrintCsvHeader(reportFormat, keys, numExpectedAttributes);

            while (keepGoing) {
                long start = System.currentTimeMillis();
                Map<String, Object> attributes = queryAttributes(conn, found, attributesInclude);
                attributes.put("time", dateFormat.isPresent() ? dateFormat.get().format(new Date()) : String.valueOf(System.currentTimeMillis()));
                maybePrintDataRows(reportFormat, numExpectedAttributes, keys, attributes);
                if (options.isOneTime()) {
                    keepGoing = false;
                } else {
                    TimeUnit.MILLISECONDS.sleep(Math.max(0, options.interval() - (System.currentTimeMillis() - start)));
                }
            }
            Exit.exit(0);
        } catch (TerseException e) {
            System.err.println(e.getMessage());
            Exit.exit(1);
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            Exit.exit(1);
        }
    }

    private static String mkString(Stream<Object> stream, String delimiter) {
        return stream.filter(Objects::nonNull).map(Object::toString).collect(Collectors.joining(delimiter));
    }

    private static int sumValues(Map<ObjectName, Integer> numExpectedAttributes) {
        return numExpectedAttributes.values().stream().mapToInt(Integer::intValue).sum();
    }

    private static String[] attributesNames(MBeanInfo mBeanInfo) {
        return Arrays.stream(mBeanInfo.getAttributes()).map(MBeanFeatureInfo::getName).toArray(String[]::new);
    }

    private static MBeanServerConnection connectToBeanServer(JmxToolOptions options) throws Exception {
        JMXConnector connector;
        MBeanServerConnection serverConn = null;
        boolean connected = false;
        long connectTimeoutMs = 10_000;
        long connectTestStarted = System.currentTimeMillis();
        do {
            try {
                // printing to stderr because system tests parse the output
                System.err.printf("Trying to connect to JMX url: %s%n", options.jmxServiceURL());
                Map<String, Object> env = new HashMap<>();
                // ssl enable
                if (options.hasJmxSslEnableOpt()) {
                    env.put("com.sun.jndi.rmi.factory.socket", new SslRMIClientSocketFactory());
                }
                // password authentication enable
                if (options.hasJmxAuthPropOpt()) {
                    env.put(JMXConnector.CREDENTIALS, options.credentials());
                }
                connector = JMXConnectorFactory.connect(options.jmxServiceURL(), env);
                serverConn = connector.getMBeanServerConnection();
                connected = true;
            } catch (Exception e) {
                System.err.printf("Could not connect to JMX url: %s. Exception: %s.%n",
                        options.jmxServiceURL(), e.getMessage());
                e.printStackTrace();
                TimeUnit.MILLISECONDS.sleep(100);
            }
        } while (System.currentTimeMillis() - connectTestStarted < connectTimeoutMs && !connected);

        if (!connected) {
            throw new TerseException(String.format("Could not connect to JMX url %s after %d ms.",
                    options.jmxServiceURL(), connectTimeoutMs));
        }
        return serverConn;
    }

    private static Set<ObjectName> findObjects(JmxToolOptions options,
                                               MBeanServerConnection conn,
                                               List<ObjectName> queries,
                                               boolean hasPatternQueries) throws Exception {
        long waitTimeoutMs = 10_000;
        Set<ObjectName> result = new HashSet<>();
        Set<ObjectName> querySet = new HashSet<>(queries);
        BiPredicate<Set<ObjectName>, Set<ObjectName>> foundAllObjects = Set::equals;
        long start = System.currentTimeMillis();
        do {
            if (!result.isEmpty()) {
                System.err.println("Could not find all object names, retrying");
                TimeUnit.MILLISECONDS.sleep(100);
            }
            result.addAll(queryObjects(conn, queries));
        } while (!hasPatternQueries && options.hasWait() && System.currentTimeMillis() - start < waitTimeoutMs && !foundAllObjects.test(querySet, result));

        if (!hasPatternQueries && options.hasWait() && !foundAllObjects.test(querySet, result)) {
            querySet.removeAll(result);
            String missing = mkString(querySet.stream().map(Object::toString), ",");
            throw new TerseException(String.format("Could not find all requested object names after %d ms. Missing %s", waitTimeoutMs, missing));
        }
        return result;
    }

    private static Set<ObjectName> queryObjects(MBeanServerConnection conn,
                                                List<ObjectName> queries) {
        Set<ObjectName> result = new HashSet<>();
        queries.forEach(name -> {
            try {
                result.addAll(conn.queryNames(name, null));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return result;
    }

    private static Map<ObjectName, Integer> findNumExpectedAttributes(MBeanServerConnection conn,
                                                                      Optional<String[]> attributesInclude,
                                                                      boolean hasPatternQueries,
                                                                      List<ObjectName> queries,
                                                                      Set<ObjectName> found) throws Exception {
        Map<ObjectName, Integer> result = new HashMap<>();
        if (attributesInclude.isEmpty()) {
            found.forEach(objectName -> {
                try {
                    MBeanInfo mBeanInfo = conn.getMBeanInfo(objectName);
                    result.put(objectName, conn.getAttributes(objectName, attributesNames(mBeanInfo)).size());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            if (hasPatternQueries) {
                found.forEach(objectName -> {
                    try {
                        MBeanInfo mBeanInfo = conn.getMBeanInfo(objectName);
                        AttributeList attributes = conn.getAttributes(objectName, attributesNames(mBeanInfo));
                        List<ObjectName> expectedAttributes = new ArrayList<>();
                        attributes.asList().forEach(attribute -> {
                            if (Arrays.asList(attributesInclude.get()).contains(attribute.getName())) {
                                expectedAttributes.add(objectName);
                            }
                        });
                        if (expectedAttributes.size() > 0) {
                            result.put(objectName, expectedAttributes.size());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            } else {
                found.forEach(objectName -> result.put(objectName, attributesInclude.get().length));
            }
        }

        if (result.isEmpty()) {
            throw new TerseException(String.format("No matched attributes for the queried objects %s.", queries));
        }
        return result;
    }

    private static Map<String, Object> queryAttributes(MBeanServerConnection conn,
                                                       Set<ObjectName> objectNames,
                                                       Optional<String[]> attributesInclude) throws Exception {
        Map<String, Object> result = new HashMap<>();
        for (ObjectName objectName : objectNames) {
            MBeanInfo beanInfo = conn.getMBeanInfo(objectName);
            AttributeList attributes = conn.getAttributes(objectName,
                    Arrays.stream(beanInfo.getAttributes()).map(a -> a.getName()).toArray(String[]::new));
            for (Attribute attribute : attributes.asList()) {
                if (attributesInclude.isPresent()) {
                    if (Arrays.asList(attributesInclude.get()).contains(attribute.getName())) {
                        result.put(String.format("%s:%s", objectName.toString(), attribute.getName()),
                                attribute.getValue());
                    }
                } else {
                    result.put(String.format("%s:%s", objectName.toString(), attribute.getName()),
                            attribute.getValue());
                }
            }
        }
        return result;
    }

    private static void maybePrintCsvHeader(String reportFormat, List<String> keys, Map<ObjectName, Integer> numExpectedAttributes) {
        if (reportFormat.equals("original") && keys.size() == sumValues(numExpectedAttributes) + 1) {
            System.out.println(mkString(keys.stream().map(key -> String.format("\"%s\"", key)), ","));
        }
    }

    private static void maybePrintDataRows(String reportFormat, Map<ObjectName, Integer> numExpectedAttributes, List<String> keys, Map<String, Object> attributes) {
        if (attributes.size() == sumValues(numExpectedAttributes) + 1) {
            switch (reportFormat) {
                case "properties":
                    keys.forEach(key -> System.out.printf("%s=%s%n", key, attributes.get(key)));
                    break;
                case "csv":
                    keys.forEach(key -> System.out.printf("%s,\"%s\"%n", key, attributes.get(key)));
                    break;
                case "tsv":
                    keys.forEach(key -> System.out.printf("%s\t%s%n", key, attributes.get(key)));
                    break;
                default:
                    System.out.println(mkString(keys.stream().map(attributes::get), ","));
                    break;
            }
        }
    }

    private static class JmxToolOptions extends CommandDefaultOptions {
        private final OptionSpec<String> objectNameOpt;
        private final OptionSpec<String> attributesOpt;
        private final OptionSpec<Integer> reportingIntervalOpt;
        private final OptionSpec<Boolean> oneTimeOpt;
        private final OptionSpec<String> dateFormatOpt;
        private final OptionSpec<String> jmxServiceUrlOpt;
        private final OptionSpec<String> reportFormatOpt;
        private final OptionSpec<String> jmxAuthPropOpt;
        private final OptionSpec<Boolean> jmxSslEnableOpt;
        private final OptionSpec<Void> waitOpt;

        public JmxToolOptions(String[] args) {
            super(args);
            objectNameOpt = parser.accepts("object-name", "A JMX object name to use as a query. This can contain wild cards, and this option " +
                    "can be given multiple times to specify more than one query. If no objects are specified " +
                    "all objects will be queried.")
                .withRequiredArg()
                .describedAs("name")
                .ofType(String.class);
            attributesOpt = parser.accepts("attributes", "The list of attributes to include in the query. This is a comma-separated list. If no " +
                    "attributes are specified all objects will be queried.")
                .withRequiredArg()
                .describedAs("name")
                .ofType(String.class);
            reportingIntervalOpt = parser.accepts("reporting-interval", "Interval in MS with which to poll jmx stats; default value is 2 seconds. " +
                    "Value of -1 equivalent to setting one-time to true")
                .withRequiredArg()
                .describedAs("ms")
                .ofType(Integer.class)
                .defaultsTo(2000);
            oneTimeOpt = parser.accepts("one-time", "Flag to indicate run once only.")
                .withOptionalArg()
                .describedAs("one-time")
                .ofType(Boolean.class)
                .defaultsTo(false);
            dateFormatOpt = parser.accepts("date-format", "The date format to use for formatting the time field. " +
                    "See java.text.SimpleDateFormat for options.")
                .withRequiredArg()
                .describedAs("format")
                .ofType(String.class);
            jmxServiceUrlOpt = parser.accepts("jmx-url", "The url to connect to poll JMX data. See Oracle javadoc for JMXServiceURL for details.")
                .withRequiredArg()
                .describedAs("service-url")
                .ofType(String.class)
                .defaultsTo("service:jmx:rmi:///jndi/rmi://:9999/jmxrmi");
            reportFormatOpt = parser.accepts("report-format", "output format name: either 'original', 'properties', 'csv', 'tsv' ")
                .withRequiredArg()
                .describedAs("report-format")
                .ofType(String.class)
                .defaultsTo("original");
            jmxAuthPropOpt = parser.accepts("jmx-auth-prop", "A mechanism to pass property in the form 'username=password' " +
                    "when enabling remote JMX with password authentication.")
                .withRequiredArg()
                .describedAs("jmx-auth-prop")
                .ofType(String.class);
            jmxSslEnableOpt = parser.accepts("jmx-ssl-enable", "Flag to enable remote JMX with SSL.")
                .withRequiredArg()
                .describedAs("ssl-enable")
                .ofType(Boolean.class)
                .defaultsTo(false);
            waitOpt = parser.accepts("wait", "Wait for requested JMX objects to become available before starting output. " +
                "Only supported when the list of objects is non-empty and contains no object name patterns.");
            options = parser.parse(args);
        }

        public JMXServiceURL jmxServiceURL() throws MalformedURLException {
            return new JMXServiceURL(options.valueOf(jmxServiceUrlOpt));
        }

        public int interval() {
            return options.valueOf(reportingIntervalOpt);
        }

        public boolean isOneTime() {
            return interval() < 0 || options.has(oneTimeOpt);
        }

        public Optional<String[]> attributesInclude() {
            if (options.has(attributesOpt)) {
                String[] attributes = Arrays.stream(options.valueOf(attributesOpt).split(","))
                        .sequential().filter(s -> !s.isEmpty()).toArray(String[]::new);
                return Optional.of(attributes);
            } else {
                return Optional.empty();
            }
        }

        public Optional<DateFormat> dateFormat() {
            return options.has(dateFormatOpt)
                ? Optional.of(new SimpleDateFormat(options.valueOf(dateFormatOpt)))
                : Optional.empty();
        }

        public boolean hasWait() {
            return options.has(waitOpt);
        }

        private String parseFormat() {
            String reportFormat = options.valueOf(reportFormatOpt).toLowerCase(Locale.ROOT);
            return Arrays.asList("properties", "csv", "tsv").contains(reportFormat) ? reportFormat : "original";
        }

        public boolean hasJmxAuthPropOpt() {
            return options.has(jmxAuthPropOpt);
        }

        public boolean hasJmxSslEnableOpt() {
            return options.has(jmxSslEnableOpt);
        }

        public String[] credentials() {
            return options.valueOf(jmxAuthPropOpt).split("=", 2);
        }

        public List<ObjectName> queries() {
            if (options.has(objectNameOpt)) {
                return options.valuesOf(objectNameOpt).stream()
                        .map(s -> {
                            try {
                                return new ObjectName(s);
                            } catch (MalformedObjectNameException e) {
                                throw new RuntimeException(e);
                            }
                        }).toList();
            } else {
                List<ObjectName> listWithNull = new ArrayList<>();
                listWithNull.add(null);
                return listWithNull;
            }
        }
    }
}
