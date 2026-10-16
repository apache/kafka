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
package kafka.docker;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.server.config.AbstractKafkaConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The native image can only create the classes registered in reflect-config.json by reflection, and a missing
 * entry fails at runtime rather than at build time. See KAFKA-19583, KAFKA-19584 and KAFKA-21045.
 */
public class NativeImageConfigTest {

    private static final Path REFLECT_CONFIG = Path.of(System.getProperty("user.dir"),
            "..", "docker", "native", "native-image-configs", "reflect-config.json");

    private static final String HOW_TO_FIX = "Register their no-arg constructors in " + REFLECT_CONFIG
            + ", or instantiate them without reflection (see KAFKA-20825).";

    // Interfaces whose implementations may be instantiated reflectively by class name.
    // To find more, search for Utils.newInstance and getConfiguredInstance(s).
    private static final List<String> PLUGGABLE_INTERFACES = List.of(
            "javax.security.auth.spi.LoginModule",
            "javax.security.sasl.SaslClientFactory",
            "javax.security.sasl.SaslServerFactory",
            "kafka.metrics.KafkaMetricsReporter",
            "org.apache.kafka.common.config.provider.ConfigProvider",
            "org.apache.kafka.common.metrics.MetricsReporter",
            "org.apache.kafka.common.replica.ReplicaSelector",
            "org.apache.kafka.common.security.auth.AuthenticateCallbackHandler",
            "org.apache.kafka.common.security.auth.KafkaPrincipalBuilder",
            "org.apache.kafka.common.security.auth.Login",
            "org.apache.kafka.common.security.auth.SecurityProviderCreator",
            "org.apache.kafka.common.security.auth.SslEngineFactory",
            "org.apache.kafka.common.security.oauthbearer.JwtRetriever",
            "org.apache.kafka.common.security.oauthbearer.JwtValidator",
            "org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor",
            "org.apache.kafka.coordinator.group.api.assignor.ShareGroupPartitionAssignor",
            "org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescriptionPlugin",
            "org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignor",
            "org.apache.kafka.coordinator.group.streams.AssignmentRefiner",
            "org.apache.kafka.server.authorizer.Authorizer",
            "org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager",
            "org.apache.kafka.server.log.remote.storage.RemoteStorageManager",
            "org.apache.kafka.server.policy.AlterConfigPolicy",
            "org.apache.kafka.server.policy.CreateTopicPolicy",
            "org.apache.kafka.server.quota.ClientQuotaCallback"
    );

    private static Map<String, JsonNode> reflectConfig;

    @BeforeAll
    public static void loadReflectConfig() throws IOException {
        reflectConfig = new HashMap<>();
        for (JsonNode entry : new ObjectMapper().readTree(REFLECT_CONFIG.toFile())) {
            reflectConfig.put(entry.get("name").asText(), entry);
        }
    }

    @Test
    public void testBrokerConfigDefaultsAreRegistered() {
        Set<String> missing = new TreeSet<>();
        for (ConfigDef.ConfigKey key : AbstractKafkaConfig.CONFIG_DEF.configKeys().values()) {
            for (Class<?> clazz : defaultClasses(key)) {
                if (isReflectivelyInstantiable(clazz) && isNoArgConstructorUnregistered(clazz.getName(), reflectConfig))
                    missing.add(clazz.getName() + " (default of " + key.name + ")");
            }
        }
        assertEquals(Set.of(), missing, "These config defaults are instantiated reflectively. " + HOW_TO_FIX);
    }

    @Test
    public void testPluginImplementationsAreRegistered() throws ClassNotFoundException {
        assertEquals(Set.of(), findUnregisteredPlugins(reflectConfig),
                "These plugins may be instantiated reflectively. " + HOW_TO_FIX);
    }

    @Test
    public void testUnregisteredPluginIsDetected() throws ClassNotFoundException {
        String reporter = "org.apache.kafka.common.metrics.JmxReporter";
        Map<String, JsonNode> incompleteConfig = new HashMap<>(reflectConfig);
        incompleteConfig.remove(reporter);

        assertEquals(Set.of(reporter + " (implements org.apache.kafka.common.metrics.MetricsReporter)"),
                findUnregisteredPlugins(incompleteConfig));
    }

    @Test
    public void testUnregisteredPluginConstructorIsDetected() throws ClassNotFoundException {
        String reporter = "org.apache.kafka.common.metrics.JmxReporter";
        Map<String, JsonNode> incompleteConfig = new HashMap<>(reflectConfig);
        incompleteConfig.put(reporter, new ObjectMapper().createObjectNode().put("name", reporter));

        assertEquals(Set.of(reporter + " (implements org.apache.kafka.common.metrics.MetricsReporter)"),
                findUnregisteredPlugins(incompleteConfig));
    }

    @Test
    public void testRegisteredKafkaClassesExist() {
        Set<String> unknown = new TreeSet<>();
        for (String className : reflectConfig.keySet()) {
            if (!isKafkaClass(className)) {
                continue;
            }
            try {
                Class.forName(className, false, NativeImageConfigTest.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                unknown.add(className);
            }
        }
        assertEquals(Set.of(), unknown, "These classes are registered in " + REFLECT_CONFIG
                + " but don't exist. Were they renamed or removed?");
    }

    private static Set<String> findUnregisteredPlugins(Map<String, JsonNode> registrations) throws ClassNotFoundException {
        Set<String> missing = new TreeSet<>();
        try (ScanResult scan = new ClassGraph()
                .overrideClassLoaders(NativeImageConfigTest.class.getClassLoader())
                .filterClasspathElements(NativeImageConfigTest::isMainClasspathElement)
                .acceptPackages("kafka", "org.apache.kafka")
                .enableClassInfo()
                .scan()) {
            for (String name : PLUGGABLE_INTERFACES) {
                Class<?> iface = Class.forName(name, false, NativeImageConfigTest.class.getClassLoader());
                for (ClassInfo classInfo : scan.getClassesImplementing(iface.getName())) {
                    String className = classInfo.getName();
                    Class<?> clazz = Class.forName(className, false, NativeImageConfigTest.class.getClassLoader());
                    if (isReflectivelyInstantiable(clazz) && isNoArgConstructorUnregistered(className, registrations))
                        missing.add(className + " (implements " + iface.getName() + ")");
                }
            }
        }
        return missing;
    }

    private static List<Class<?>> defaultClasses(ConfigDef.ConfigKey key) {
        if (key.defaultValue instanceof Class<?> clazz)
            return List.of(clazz);

        List<?> values;
        if (key.defaultValue instanceof String value)
            values = List.of(value);
        else if (key.defaultValue instanceof List<?> list)
            values = list;
        else
            return List.of();

        List<Class<?>> classes = new ArrayList<>();
        for (Object value : values) {
            if (!(value instanceof String name))
                continue;
            try {
                classes.add(Class.forName(name, false, NativeImageConfigTest.class.getClassLoader()));
            } catch (ClassNotFoundException e) {
                // STRING and LIST defaults may contain values which are not class names.
            }
        }
        return classes;
    }

    // Utils.newInstance requires a public class with a public no-arg constructor
    private static boolean isReflectivelyInstantiable(Class<?> clazz) {
        int modifiers = clazz.getModifiers();
        if (!Modifier.isPublic(modifiers) || Modifier.isAbstract(modifiers))
            return false;
        try {
            clazz.getConstructor();
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static boolean isMainClasspathElement(String entry) {
        Path path = Path.of(entry);
        String fileName = path.getFileName().toString();
        if (Files.isDirectory(path))
            return entry.contains(File.separator + "build" + File.separator + "classes" + File.separator)
                    && fileName.equals("main");
        return fileName.startsWith("kafka") && fileName.endsWith(".jar") && !fileName.contains("test");
    }

    private static boolean isKafkaClass(String className) {
        String name = className.startsWith("[L") ? className.substring(2, className.length() - 1) : className;
        return name.startsWith("org.apache.kafka.") || name.startsWith("kafka.");
    }

    private static boolean isNoArgConstructorUnregistered(String className, Map<String, JsonNode> registrations) {
        JsonNode entry = registrations.get(className);
        if (entry == null)
            return true;
        if (entry.path("allDeclaredConstructors").asBoolean() || entry.path("allPublicConstructors").asBoolean())
            return false;
        for (JsonNode method : entry.path("methods")) {
            if ("<init>".equals(method.path("name").asText()) && method.path("parameterTypes").isEmpty())
                return false;
        }
        return true;
    }
}
