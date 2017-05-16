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

import java.lang.reflect.Modifier;

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

    public static boolean shouldLoadInIsolation(String name) {
        return !(name.matches(BLACKLIST) && !name.matches(WHITELIST));
    }

    public static boolean isConcrete(Class<?> klass) {
        int mod = klass.getModifiers();
        return !Modifier.isAbstract(mod) && !Modifier.isInterface(mod);
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

    private static String prunePluginName(PluginDesc<?> plugin, String suffix) {
        String simple = plugin.pluginClass().getSimpleName();
        int pos = simple.lastIndexOf(suffix);
        if (pos > 0) {
            return simple.substring(0, pos);
        }
        return simple;
    }

}
