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

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class PluginUtils {
    private static final String BLACKLIST = "^(?:"
            + "java"
            + "|javax"
            + "|org\\.omg"
            + "|org\\.w3c\\.dom"
            + "|org\\.apache\\.kafka\\.common"
            + "|org\\.apache\\.kafka\\.connect"
            + "|org\\.apache\\.log4j"
            + ")\\..*$";

    private static final String WHITELIST = "^org\\.apache\\.kafka\\.connect\\.(?:"
            + "transforms\\.(?!Transformation$).*"
            + "|json\\..*"
            + "|file\\..*"
            + "|converters\\..*"
            + "|storage\\.StringConverter"
            + ")$";

    private static final DirectoryStream.Filter<Path> PLUGIN_PATH_FILTER = new DirectoryStream
            .Filter<Path>() {
        @Override
        public boolean accept(Path path) throws IOException {
            return Files.isDirectory(path) || PluginUtils.isJar(path);
        }
    };

    public static boolean shouldLoadInIsolation(String name) {
        return !(name.matches(BLACKLIST) && !name.matches(WHITELIST));
    }

    public static boolean isConcrete(Class<?> klass) {
        int mod = klass.getModifiers();
        return !Modifier.isAbstract(mod) && !Modifier.isInterface(mod);
    }

    public static boolean isJar(Path path) {
        return path.toString().toLowerCase(Locale.ROOT).endsWith(".jar");
    }

    public static List<URL> pluginUrls(Path pluginPath) throws IOException {
        List<URL> urls = new ArrayList<>();
        if (PluginUtils.isJar(pluginPath)) {
            urls.add(pluginPath.toUri().toURL());
        } else if (Files.isDirectory(pluginPath)) {
            try (
                    DirectoryStream<Path> listing = Files.newDirectoryStream(
                            pluginPath,
                            PLUGIN_PATH_FILTER
                    )
            ) {
                for (Path jar : listing) {
                    urls.add(jar.toUri().toURL());
                }
            }
        }
        return urls;
    }

    public static List<Path> pluginLocations(Path topPath) throws IOException {
        List<Path> locations = new ArrayList<>();
        // Non-recursive for now. Plugin directories or jars need to be exactly under the topPath.
        try (
                DirectoryStream<Path> listing = Files.newDirectoryStream(
                        topPath,
                        PLUGIN_PATH_FILTER
                )
        ) {
            for (Path dir : listing) {
                locations.add(dir);
            }
        }
        return locations;
    }

    public static String simpleName(PluginDesc<?> plugin) {
        return plugin.pluginClass().getSimpleName();
    }

    public static String prunedName(PluginDesc<?> plugin) {
        // It's currently simpler to switch on type than do pattern matching.
        switch (plugin.type()) {
            case SOURCE:
            case SINK:
            case CONNECTOR:
                return prunePluginName(plugin, "Connector");
            default:
                return prunePluginName(plugin, plugin.type().simpleName());
        }
    }

    public static <U> boolean isAliasUnique(
            PluginDesc<U> alias,
            Collection<PluginDesc<U>> plugins
    ) {
        boolean matched = false;
        for (PluginDesc<U> plugin : plugins) {
            if (simpleName(alias).equals(simpleName(plugin))
                    || prunedName(alias).equals(prunedName(plugin))) {
                if (matched) {
                    return false;
                }
                matched = true;
            }
        }
        return true;
    }

    private static String prunePluginName(PluginDesc<?> plugin, String suffix) {
        String simple = plugin.pluginClass().getSimpleName();
        int pos = simple.lastIndexOf(suffix);
        if (pos > 0) {
            return simple.substring(0, pos);
        }
        return simple;
    }

}
