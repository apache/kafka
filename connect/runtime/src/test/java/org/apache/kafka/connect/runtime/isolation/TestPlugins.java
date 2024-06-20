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

package org.apache.kafka.connect.runtime.isolation;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.kafka.connect.components.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for constructing test plugins for Connect.
 *
 * <p>Plugins are built from their source under resources/test-plugins/ and placed into temporary
 * jar files that are deleted when the process exits.
 *
 * <p>To add a plugin, create the source files in the resource tree, and edit this class to build
 * that plugin during initialization. For example, the plugin class {@literal package.Class} should
 * be placed in {@literal resources/test-plugins/something/package/Class.java} and added as a
 * TestPlugin enum {@code PLUGIN_ID("something", "package.Class");}. The class name, contents,
 * and plugin directory can take any value you need for testing.
 *
 * <p>You can then assemble a {@literal plugin.path} of a list of plugin jars via {@link TestPlugins#pluginPath},
 * and reference the names of the different plugins directly via the {@link TestPlugin} enum.
 */
public class TestPlugins {
    private static final Predicate<String> REMOVE_CLASS_FILTER = s -> s.contains("NonExistentInterface");
    public enum TestPlugin {
        /**
         * A plugin which will always throw an exception during loading
         */
        ALWAYS_THROW_EXCEPTION("always-throw-exception", "test.plugins.AlwaysThrowException", false),
        /**
         * A plugin which samples information about its initialization.
         */
        ALIASED_STATIC_FIELD("aliased-static-field", "test.plugins.AliasedStaticField"),
        /**
         * A {@link org.apache.kafka.connect.storage.Converter}
         * which samples information about its method calls.
         */
        SAMPLING_CONVERTER("sampling-converter", "test.plugins.SamplingConverter"),
        /**
         * A {@link org.apache.kafka.common.Configurable}
         * which samples information about its method calls.
         */
        SAMPLING_CONFIGURABLE("sampling-configurable", "test.plugins.SamplingConfigurable"),
        /**
         * A {@link org.apache.kafka.connect.storage.HeaderConverter}
         * which samples information about its method calls.
         */
        SAMPLING_HEADER_CONVERTER("sampling-header-converter", "test.plugins.SamplingHeaderConverter"),
        /**
         * A {@link org.apache.kafka.common.config.provider.ConfigProvider}
         * which samples information about its method calls.
         */
        SAMPLING_CONFIG_PROVIDER("sampling-config-provider", "test.plugins.SamplingConfigProvider"),
        /**
         * A {@link org.apache.kafka.connect.sink.SinkConnector}
         * which samples information about its method calls.
         */
        SAMPLING_CONNECTOR("sampling-connector", "test.plugins.SamplingConnector"),
        /**
         * A plugin which uses a {@link java.util.ServiceLoader}
         * to load internal classes, and samples information about their initialization.
         */
        SERVICE_LOADER("service-loader", "test.plugins.ServiceLoaderPlugin"),
        /**
         * A plugin which reads a version string from a resource and packages the version string 1.0.0.
         */
        READ_VERSION_FROM_RESOURCE_V1("read-version-from-resource-v1", "test.plugins.ReadVersionFromResource"),
        /**
         * A plugin which reads a version string from a resource and packages the version string 2.0.0.
         * This plugin is not included in {@link TestPlugins#pluginPath()} and must be included explicitly
         */
        READ_VERSION_FROM_RESOURCE_V2("read-version-from-resource-v2", "test.plugins.ReadVersionFromResource", false),
        /**
         * A plugin which shares a jar file with {@link TestPlugin#MULTIPLE_PLUGINS_IN_JAR_THING_TWO}
         */
        MULTIPLE_PLUGINS_IN_JAR_THING_ONE("multiple-plugins-in-jar", "test.plugins.ThingOne"),
        /**
         * A plugin which shares a jar file with {@link TestPlugin#MULTIPLE_PLUGINS_IN_JAR_THING_ONE}
         */
        MULTIPLE_PLUGINS_IN_JAR_THING_TWO("multiple-plugins-in-jar", "test.plugins.ThingTwo"),
        /**
         * A plugin which is incorrectly packaged, and is missing a superclass definition.
         */
        BAD_PACKAGING_MISSING_SUPERCLASS("bad-packaging", "test.plugins.MissingSuperclassConverter", false, REMOVE_CLASS_FILTER),
        /**
         * A plugin which is packaged with other incorrectly packaged plugins, but itself has no issues loading.
         */
        BAD_PACKAGING_CO_LOCATED("bad-packaging", "test.plugins.CoLocatedPlugin", true, REMOVE_CLASS_FILTER),
        /**
         * A connector which is incorrectly packaged, and throws during static initialization.
         */
        BAD_PACKAGING_STATIC_INITIALIZER_THROWS_CONNECTOR("bad-packaging", "test.plugins.StaticInitializerThrowsConnector", false, REMOVE_CLASS_FILTER),
        /**
         * A plugin which is incorrectly packaged, which throws an exception from the {@link Versioned#version()} method.
         */
        BAD_PACKAGING_VERSION_METHOD_THROWS_CONNECTOR("bad-packaging", "test.plugins.VersionMethodThrowsConnector", true, REMOVE_CLASS_FILTER),
        /**
         * A plugin which is incorrectly packaged, which throws an exception from default constructor.
         */
        BAD_PACKAGING_DEFAULT_CONSTRUCTOR_THROWS_CONNECTOR("bad-packaging", "test.plugins.DefaultConstructorThrowsConnector", false, REMOVE_CLASS_FILTER),
        /**
         * A plugin which is incorrectly packaged, which has a private default constructor.
         */
        BAD_PACKAGING_DEFAULT_CONSTRUCTOR_PRIVATE_CONNECTOR("bad-packaging", "test.plugins.DefaultConstructorPrivateConnector", false, REMOVE_CLASS_FILTER),
        /**
         * A plugin which is incorrectly packaged, which has a constructor which takes arguments.
         */
        BAD_PACKAGING_NO_DEFAULT_CONSTRUCTOR_CONNECTOR("bad-packaging", "test.plugins.NoDefaultConstructorConnector", false, REMOVE_CLASS_FILTER),
        /**
         * A plugin which is incorrectly packaged, which has a constructor which takes arguments.
         */
        BAD_PACKAGING_NO_DEFAULT_CONSTRUCTOR_CONVERTER("bad-packaging", "test.plugins.NoDefaultConstructorConverter", false, REMOVE_CLASS_FILTER),
        /**
         * A plugin which is incorrectly packaged, which has a constructor which takes arguments.
         */
        BAD_PACKAGING_NO_DEFAULT_CONSTRUCTOR_OVERRIDE_POLICY("bad-packaging", "test.plugins.NoDefaultConstructorOverridePolicy", false, REMOVE_CLASS_FILTER),
        /**
         * A plugin which is incorrectly packaged, which throws an exception from the {@link Versioned#version()} method.
         */
        BAD_PACKAGING_INNER_CLASS_CONNECTOR("bad-packaging", "test.plugins.OuterClass$InnerClass", false, REMOVE_CLASS_FILTER),
        /**
         * A plugin which is incorrectly packaged, which throws an exception from the {@link Versioned#version()} method.
         */
        BAD_PACKAGING_STATIC_INITIALIZER_THROWS_REST_EXTENSION("bad-packaging", "test.plugins.StaticInitializerThrowsRestExtension", false, REMOVE_CLASS_FILTER),
        /**
         * A reflectively discovered plugin which subclasses another plugin which is present on the classpath
         */
        SUBCLASS_OF_CLASSPATH_CONVERTER("subclass-of-classpath", "test.plugins.SubclassOfClasspathConverter"),
        /**
         * A ServiceLoader discovered plugin which subclasses another plugin which is present on the classpath
         */
        SUBCLASS_OF_CLASSPATH_OVERRIDE_POLICY("subclass-of-classpath", "test.plugins.SubclassOfClasspathOverridePolicy"),
        /**
         * A converter which does not have a corresponding ServiceLoader manifest
         */
        NON_MIGRATED_CONVERTER("non-migrated", "test.plugins.NonMigratedConverter", false),
        /**
         * A header converter which does not have a corresponding ServiceLoader manifest
         */
        NON_MIGRATED_HEADER_CONVERTER("non-migrated", "test.plugins.NonMigratedHeaderConverter", false),
        /**
         * A plugin which implements multiple interfaces, and has ServiceLoader manifests for some interfaces and not others.
         */
        NON_MIGRATED_MULTI_PLUGIN("non-migrated", "test.plugins.NonMigratedMultiPlugin", false),
        /**
         * A predicate which does not have a corresponding ServiceLoader manifest
         */
        NON_MIGRATED_PREDICATE("non-migrated", "test.plugins.NonMigratedPredicate", false),
        /**
         * A sink connector which does not have a corresponding ServiceLoader manifest
         */
        NON_MIGRATED_SINK_CONNECTOR("non-migrated", "test.plugins.NonMigratedSinkConnector", false),
        /**
         * A source connector which does not have a corresponding ServiceLoader manifest
         */
        NON_MIGRATED_SOURCE_CONNECTOR("non-migrated", "test.plugins.NonMigratedSourceConnector", false),
        /**
         * A transformation which does not have a corresponding ServiceLoader manifest
         */
        NON_MIGRATED_TRANSFORMATION("non-migrated", "test.plugins.NonMigratedTransformation", false);

        private final String resourceDir;
        private final String className;
        private final boolean includeByDefault;
        private final Predicate<String> removeRuntimeClasses;

        TestPlugin(String resourceDir, String className) {
            this(resourceDir, className, true);
        }

        TestPlugin(String resourceDir, String className, boolean includeByDefault) {
            this(resourceDir, className, includeByDefault, ignored -> false);
        }

        TestPlugin(String resourceDir, String className, boolean includeByDefault, Predicate<String> removeRuntimeClasses) {
            this.resourceDir = resourceDir;
            this.className = className;
            this.includeByDefault = includeByDefault;
            this.removeRuntimeClasses = removeRuntimeClasses;
        }

        public String resourceDir() {
            return resourceDir;
        }

        public String className() {
            return className;
        }

        public boolean includeByDefault() {
            return includeByDefault;
        }

        public Predicate<String> removeRuntimeClasses() {
            return removeRuntimeClasses;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TestPlugins.class);
    private static final Map<String, Path> PLUGIN_JARS;
    private static final Throwable INITIALIZATION_EXCEPTION;

    static {
        Throwable err = null;
        Map<String, Path> pluginJars = new HashMap<>();
        try {
            for (TestPlugin testPlugin : TestPlugin.values()) {
                if (pluginJars.containsKey(testPlugin.resourceDir())) {
                    log.debug("Skipping recompilation of " + testPlugin.resourceDir());
                }
                pluginJars.put(testPlugin.resourceDir(), createPluginJar(testPlugin.resourceDir(), testPlugin.removeRuntimeClasses()));
            }
        } catch (Throwable e) {
            log.error("Could not set up plugin test jars", e);
            err = e;
        }
        PLUGIN_JARS = Collections.unmodifiableMap(pluginJars);
        INITIALIZATION_EXCEPTION = err;
    }

    private static void assertAvailable() throws AssertionError {
        if (INITIALIZATION_EXCEPTION != null) {
            throw new AssertionError("TestPlugins did not initialize completely",
                INITIALIZATION_EXCEPTION);
        }
        if (PLUGIN_JARS.isEmpty()) {
            throw new AssertionError("No test plugins loaded");
        }
    }

    /**
     * Assemble a default plugin path containing all TestPlugin instances which are not hidden by default.
     * @return A list of plugin jar filenames
     * @throws AssertionError if any plugin failed to load, or no plugins were loaded.
     */
    public static Set<Path> pluginPath() {
        return pluginPath(defaultPlugins());
    }

    public static String pluginPathJoined() {
        return pluginPath().stream().map(Path::toString).collect(Collectors.joining(","));
    }

    /**
     * Assemble a plugin path containing some TestPlugin instances
     * @param plugins One or more plugins which should be included on the plugin path.
     * @return A list of plugin jar filenames containing the specified test plugins
     * @throws AssertionError if any plugin failed to load, or no plugins were loaded.
     */
    public static Set<Path> pluginPath(TestPlugin... plugins) {
        assertAvailable();
        return Arrays.stream(plugins)
                .filter(Objects::nonNull)
                .map(TestPlugin::resourceDir)
                .distinct()
                .map(PLUGIN_JARS::get)
                .collect(Collectors.toSet());
    }

    public static String pluginPathJoined(TestPlugin... plugins) {
        return pluginPath(plugins).stream().map(Path::toString).collect(Collectors.joining(","));
    }

    /**
     * Get all plugin classes which are included on the default classpath
     * @return A list of plugin class names
     * @throws AssertionError if any plugin failed to load, or no plugins were loaded.
     */
    public static List<String> pluginClasses() {
        return pluginClasses(defaultPlugins());
    }

    /**
     * Get all plugin classes which are included in the specified plugins
     * @param plugins One or more plugins which are included in the plugin path.
     * @return A list of plugin class names
     * @throws AssertionError if any plugin failed to load, or no plugins were loaded.
     */
    public static List<String> pluginClasses(TestPlugin... plugins) {
        assertAvailable();
        return Arrays.stream(plugins)
                .filter(Objects::nonNull)
                .map(TestPlugin::className)
                .distinct()
                .collect(Collectors.toList());
    }

    private static TestPlugin[] defaultPlugins() {
        return Arrays.stream(TestPlugin.values())
                .filter(TestPlugin::includeByDefault)
                .toArray(TestPlugin[]::new);
    }

    private static Path createPluginJar(String resourceDir, Predicate<String> removeRuntimeClasses) throws IOException {
        Path inputDir = resourceDirectoryPath("test-plugins/" + resourceDir);
        Path binDir = Files.createTempDirectory(resourceDir + ".bin.");
        compileJavaSources(inputDir, binDir);
        Path jarFile = Files.createTempFile(resourceDir + ".", ".jar");
        try (JarOutputStream jar = openJarFile(jarFile)) {
            writeJar(jar, inputDir, removeRuntimeClasses);
            writeJar(jar, binDir, removeRuntimeClasses);
        }
        removeDirectory(binDir);
        jarFile.toFile().deleteOnExit();
        return jarFile;
    }

    private static Path resourceDirectoryPath(String resourceDir) throws IOException {
        URL resource = Thread.currentThread()
            .getContextClassLoader()
            .getResource(resourceDir);
        if (resource == null) {
            throw new IOException("Could not find test plugin resource: " + resourceDir);
        }
        Path file = Paths.get(resource.getFile());
        if (!Files.isDirectory(file)) {
            throw new IOException("Resource is not a directory: " + resourceDir);
        }
        if (!Files.isReadable(file)) {
            throw new IOException("Resource directory is not readable: " + resourceDir);
        }
        return file;
    }

    private static JarOutputStream openJarFile(Path jarFile) throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        return new JarOutputStream(Files.newOutputStream(jarFile), manifest);
    }

    private static void removeDirectory(Path binDir) throws IOException {
        List<Path> classFiles = Files.walk(binDir)
            .sorted(Comparator.reverseOrder())
            .collect(Collectors.toList());
        for (Path classFile : classFiles) {
            if (!Files.deleteIfExists(classFile)) {
                throw new IOException("Could not delete: " + classFile);
            }
        }
    }

    /**
     * Compile a directory of .java source files into .class files
     * .class files are placed into the same directory as their sources.
     *
     * <p>Dependencies between source files in this directory are resolved against one another
     * and the classes present in the test environment.
     * See https://stackoverflow.com/questions/1563909/ for more information.
     * Additional dependencies in your plugins should be added as test scope to :connect:runtime.
     * @param sourceDir Directory containing java source files
     * @throws IOException if the files cannot be compiled
     */
    private static void compileJavaSources(Path sourceDir, Path binDir) throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        List<File> sourceFiles = Files.walk(sourceDir)
            .filter(Files::isRegularFile)
            .filter(path -> path.toFile().getName().endsWith(".java"))
            .map(Path::toFile)
            .collect(Collectors.toList());
        StringWriter writer = new StringWriter();
        List<String> options = Arrays.asList(
            "-d", binDir.toString() // Write class output to a different directory.
        );

        try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null)) {
            boolean success = compiler.getTask(
                writer,
                fileManager,
                null,
                options,
                null,
                fileManager.getJavaFileObjectsFromFiles(sourceFiles)
            ).call();
            if (!success) {
                throw new RuntimeException("Failed to compile test plugin:\n" + writer);
            }
        }
    }

    private static void writeJar(JarOutputStream jar, Path inputDir, Predicate<String> removeRuntimeClasses) throws IOException {
        List<Path> paths = Files.walk(inputDir)
            .filter(Files::isRegularFile)
            .filter(path -> !path.toFile().getName().endsWith(".java"))
            .filter(path -> !removeRuntimeClasses.test(path.toFile().getName()))
            .collect(Collectors.toList());
        for (Path path : paths) {
            try (InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
                jar.putNextEntry(new JarEntry(
                    inputDir.relativize(path)
                        .toFile()
                        .getPath()
                        .replace(File.separator, "/")
                ));
                byte[] buffer = new byte[1024];
                for (int count; (count = in.read(buffer)) != -1; ) {
                    jar.write(buffer, 0, count);
                }
                jar.closeEntry();
            }
        }
    }

}
