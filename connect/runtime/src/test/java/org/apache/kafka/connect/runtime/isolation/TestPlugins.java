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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
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
 * be placed in {@literal resources/test-plugins/something/package/Class.java} and loaded using
 * {@code createPluginJar("something")}. The class name, contents, and plugin directory can take
 * any value you need for testing.
 *
 * <p>To use this class in your tests, make sure to first call
 * {@link TestPlugins#assertAvailable()} to verify that the plugins initialized correctly.
 * Otherwise, exceptions during the plugin build are not propagated, and may invalidate your test.
 * You can access the list of plugin jars for assembling a {@literal plugin.path}, and reference
 * the names of the different plugins directly via the exposed constants.
 */
public class TestPlugins {

    /**
     * Class name of a plugin which will always throw an exception during loading
     */
    public static final String ALWAYS_THROW_EXCEPTION = "test.plugins.AlwaysThrowException";
    /**
     * Class name of a plugin which samples information about its initialization.
     */
    public static final String ALIASED_STATIC_FIELD = "test.plugins.AliasedStaticField";
    /**
     * Class name of a {@link org.apache.kafka.connect.storage.Converter}
     * which samples information about its method calls.
     */
    public static final String SAMPLING_CONVERTER = "test.plugins.SamplingConverter";
    /**
     * Class name of a {@link org.apache.kafka.common.Configurable}
     * which samples information about its method calls.
     */
    public static final String SAMPLING_CONFIGURABLE = "test.plugins.SamplingConfigurable";
    /**
     * Class name of a {@link org.apache.kafka.connect.storage.HeaderConverter}
     * which samples information about its method calls.
     */
    public static final String SAMPLING_HEADER_CONVERTER = "test.plugins.SamplingHeaderConverter";
    /**
     * Class name of a {@link org.apache.kafka.common.config.provider.ConfigProvider}
     * which samples information about its method calls.
     */
    public static final String SAMPLING_CONFIG_PROVIDER = "test.plugins.SamplingConfigProvider";
    /**
     * Class name of a plugin which uses a {@link java.util.ServiceLoader}
     * to load internal classes, and samples information about their initialization.
     */
    public static final String SERVICE_LOADER = "test.plugins.ServiceLoaderPlugin";

    private static final Logger log = LoggerFactory.getLogger(TestPlugins.class);
    private static final Map<String, File> PLUGIN_JARS;
    private static final Throwable INITIALIZATION_EXCEPTION;

    static {
        Throwable err = null;
        HashMap<String, File> pluginJars = new HashMap<>();
        try {
            pluginJars.put(ALWAYS_THROW_EXCEPTION, createPluginJar("always-throw-exception"));
            pluginJars.put(ALIASED_STATIC_FIELD, createPluginJar("aliased-static-field"));
            pluginJars.put(SAMPLING_CONVERTER, createPluginJar("sampling-converter"));
            pluginJars.put(SAMPLING_CONFIGURABLE, createPluginJar("sampling-configurable"));
            pluginJars.put(SAMPLING_HEADER_CONVERTER, createPluginJar("sampling-header-converter"));
            pluginJars.put(SAMPLING_CONFIG_PROVIDER, createPluginJar("sampling-config-provider"));
            pluginJars.put(SERVICE_LOADER, createPluginJar("service-loader"));
        } catch (Throwable e) {
            log.error("Could not set up plugin test jars", e);
            err = e;
        }
        PLUGIN_JARS = Collections.unmodifiableMap(pluginJars);
        INITIALIZATION_EXCEPTION = err;
    }

    /**
     * Ensure that the test plugin JARs were assembled without error before continuing.
     * @throws AssertionError if any plugin failed to load, or no plugins were loaded.
     */
    public static void assertAvailable() throws AssertionError {
        if (INITIALIZATION_EXCEPTION != null) {
            throw new AssertionError("TestPlugins did not initialize completely",
                INITIALIZATION_EXCEPTION);
        }
        if (PLUGIN_JARS.isEmpty()) {
            throw new AssertionError("No test plugins loaded");
        }
    }

    /**
     * A list of jar files containing test plugins
     * @return A list of plugin jar filenames
     */
    public static List<String> pluginPath() {
        return PLUGIN_JARS.values()
            .stream()
            .map(File::getPath)
            .collect(Collectors.toList());
    }

    /**
     * Get all of the classes that were successfully built by this class
     * @return A list of plugin class names
     */
    public static List<String> pluginClasses() {
        return new ArrayList<>(PLUGIN_JARS.keySet());
    }

    private static File createPluginJar(String resourceDir) throws IOException {
        Path inputDir = resourceDirectoryPath("test-plugins/" + resourceDir);
        Path binDir = Files.createTempDirectory(resourceDir + ".bin.");
        compileJavaSources(inputDir, binDir);
        File jarFile = Files.createTempFile(resourceDir + ".", ".jar").toFile();
        try (JarOutputStream jar = openJarFile(jarFile)) {
            writeJar(jar, inputDir);
            writeJar(jar, binDir);
        }
        removeDirectory(binDir);
        jarFile.deleteOnExit();
        return jarFile;
    }

    private static Path resourceDirectoryPath(String resourceDir) throws IOException {
        URL resource = Thread.currentThread()
            .getContextClassLoader()
            .getResource(resourceDir);
        if (resource == null) {
            throw new IOException("Could not find test plugin resource: " + resourceDir);
        }
        File file = new File(resource.getFile());
        if (!file.isDirectory()) {
            throw new IOException("Resource is not a directory: " + resourceDir);
        }
        if (!file.canRead()) {
            throw new IOException("Resource directory is not readable: " + resourceDir);
        }
        return file.toPath();
    }

    private static JarOutputStream openJarFile(File jarFile) throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        return new JarOutputStream(new FileOutputStream(jarFile), manifest);
    }

    private static void removeDirectory(Path binDir) throws IOException {
        List<File> classFiles = Files.walk(binDir)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .collect(Collectors.toList());
        for (File classFile : classFiles) {
            if (!classFile.delete()) {
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

    private static void writeJar(JarOutputStream jar, Path inputDir) throws IOException {
        List<Path> paths = Files.walk(inputDir)
            .filter(Files::isRegularFile)
            .filter(path -> !path.toFile().getName().endsWith(".java"))
            .collect(Collectors.toList());
        for (Path path : paths) {
            try (InputStream in = new BufferedInputStream(new FileInputStream(path.toFile()))) {
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
