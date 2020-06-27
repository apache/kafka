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

import java.io.PrintStream;
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

    private static final List<DocInfo> PREDICATES;
    static {
        List<DocInfo> collect = new Plugins(Collections.emptyMap()).predicates().stream()
            .map(p -> {
                try {
                    String overviewDoc = (String) p.pluginClass().getDeclaredField("OVERVIEW_DOC").get(null);
                    ConfigDef configDef = (ConfigDef) p.pluginClass().getDeclaredField("CONFIG_DEF").get(null);
                    return new DocInfo(p.pluginClass(), overviewDoc, configDef);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException("Predicate class " + p.pluginClass().getName() + " lacks either a `public static final String OVERVIEW_DOC` or `public static final ConfigDef CONFIG_DEF`");
                }
            })
            .collect(Collectors.toList());
        collect.sort(Comparator.comparing(docInfo -> docInfo.predicateName));
        PREDICATES = collect;
    }

    private static void printPredicateHtml(PrintStream out, DocInfo docInfo) {
        out.println("<div id=\"" + docInfo.predicateName + "\">");

        out.print("<h5>");
        out.print(docInfo.predicateName);
        out.println("</h5>");

        out.println(docInfo.overview);

        out.println("<p/>");

        out.println(docInfo.configDef.toHtml(6, key -> docInfo.predicateName + "_" + key));

        out.println("</div>");
    }

    private static void printHtml(PrintStream out) {
        for (final DocInfo docInfo : PREDICATES) {
            printPredicateHtml(out, docInfo);
        }
    }

    public static void main(String... args) {
        printHtml(System.out);
    }
}
