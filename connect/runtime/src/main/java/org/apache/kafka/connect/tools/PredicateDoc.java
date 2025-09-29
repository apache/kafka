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
package org.apache.kafka.connect.tools;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class PredicateDoc {

    private static final class DocInfo {
        final String predicateName;
        final String overview;
        final ConfigDef configDef;

        private <P extends Predicate<?>> DocInfo(Class<P> predicateClass, String overview, ConfigDef configDef) {
            this.predicateName = predicateClass.getName();
            this.overview = overview;
            this.configDef = configDef;
        }
    }

    private static final List<DocInfo> PREDICATES = new Plugins(Collections.emptyMap()).predicates().stream()
        .map(p -> {
            try {
                String overviewDoc = (String) p.pluginClass().getDeclaredField("OVERVIEW_DOC").get(null);
                ConfigDef configDef = (ConfigDef) p.pluginClass().getDeclaredField("CONFIG_DEF").get(null);
                return new DocInfo(p.pluginClass(), overviewDoc, configDef);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Predicate class " + p.pluginClass().getName() + " lacks either a `public static final String OVERVIEW_DOC` or `public static final ConfigDef CONFIG_DEF`");
            }
        })
        .sorted(Comparator.comparing(docInfo -> docInfo.predicateName))
        .collect(Collectors.toList());

    private static String toHtml() {
        StringBuilder b = new StringBuilder();
        for (final DocInfo docInfo : PREDICATES) {
            b.append("<div id=\"" + docInfo.predicateName + "\">\n");
            b.append("<h5>");
            b.append("<a href=\"#" + docInfo.predicateName + "\">" + docInfo.predicateName + "</a>");
            b.append("</h5>\n");
            b.append(docInfo.overview + "\n");
            b.append("<p/>\n");
            b.append(docInfo.configDef.toHtml(6, key -> docInfo.predicateName + "_" + key) + "\n");
            b.append("</div>\n");
        }
        return b.toString();
    }

    public static void main(String... args) {
        System.out.println(toHtml());
    }
}
