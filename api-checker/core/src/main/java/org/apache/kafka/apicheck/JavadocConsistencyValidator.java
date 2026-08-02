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
package org.apache.kafka.apicheck;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Cross-validates the published javadoc HTML against the project's bytecode-level audience
 * annotations. Two complementary checks:
 * <ul>
 *   <li>{@code MISSING_JAVADOC} — a class carries a *direct* {@code @InterfaceAudience.Public}
 *       but has no HTML page in the javadoc jar. Fired only on direct annotation because
 *       javadoc doesn't emit a separate page for inherited-Public nested classes (protected or
 *       package-private); their docs live on the outer's page.</li>
 *   <li>{@code MISSING_PUBLICAPI_ANNOTATION} — a class appears in the javadoc HTML but isn't
 *       effectively {@code @Public} (direct or inherited).</li>
 * </ul>
 *
 * <p>Deprecated classes are out of scope on both sides: the deprecation set on the
 * {@link ApiSurface} is subtracted from the HTML-discovered set before cross-validation.
 *
 * <p>Returns a {@link CheckResult} with an empty suppressions list — these violations don't
 * currently have an escape-hatch mechanism, but the uniform shape lets callers compose
 * validators without per-validator special cases.
 */
final class JavadocConsistencyValidator {

    private JavadocConsistencyValidator() {}

    static CheckResult validate(File javadocJar, ApiSurface surface) throws IOException {
        List<PublicApiViolation> violations = new ArrayList<>();

        Set<String> classesWithPublicDoc = findClassesFromJavadocHtml(javadocJar);
        classesWithPublicDoc.removeIf(surface::isDeprecated);

        for (ClassFacts facts : surface.directPublic()) {
            if (!classesWithPublicDoc.contains(facts.dottedName())) {
                violations.add(new PublicApiViolation(
                    facts.dottedName(),
                    "MISSING_JAVADOC",
                    "Class has @InterfaceAudience.Public annotation but is missing from javadoc",
                    null));
            }
        }

        for (String dottedName : classesWithPublicDoc) {
            if (!surface.isEffectivelyPublic(dottedName)) {
                violations.add(new PublicApiViolation(
                    dottedName,
                    "MISSING_PUBLICAPI_ANNOTATION",
                    "Class appears in javadoc but lacks @InterfaceAudience.Public annotation",
                    null));
            }
        }

        return new CheckResult(violations, List.of());
    }

    /** Find Kafka-namespaced class names from HTML files in a javadoc JAR. */
    private static Set<String> findClassesFromJavadocHtml(File javadocJar) throws IOException {
        Set<String> classes = new HashSet<>();
        try (JarFile jar = new JarFile(javadocJar)) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (!isClassHtmlFile(entry.getName())) continue;
                String className = convertHtmlPathToClassName(entry.getName());
                if (className.startsWith("org.apache.kafka.")) {
                    classes.add(className);
                }
            }
        }
        return classes;
    }

    /** True if the entry path is an {@code org/apache/kafka/...} class HTML page. */
    private static boolean isClassHtmlFile(String path) {
        if (!path.endsWith(".html")) return false;
        if (!path.startsWith("org/apache/kafka/")) return false;
        // Skip the auxiliary trees javadoc emits when options.use() / options.linksource() are on:
        // .../class-use/Foo.html (cross-reference index) and .../src-html/Foo.html (source listing).
        // Neither is a class page, but both would parse as one and produce a bogus
        // MISSING_PUBLICAPI_ANNOTATION for "...class-use.Foo" or "...src-html.Foo".
        if (path.contains("/class-use/") || path.contains("/src-html/")) return false;
        String fileName = path.substring(path.lastIndexOf('/') + 1);
        // Class HTML files start with an uppercase letter; structural pages (index, overview-tree,
        // package-summary, …) don't.
        String classNamePart = fileName.replaceAll(".html$", "");
        return !classNamePart.isEmpty() && Character.isUpperCase(classNamePart.charAt(0));
    }

    /** {@code org/apache/kafka/common/resource/Resource.html} → {@code org.apache.kafka.common.resource.Resource}. */
    private static String convertHtmlPathToClassName(String htmlPath) {
        return htmlPath.replace('/', '.').replaceAll(".html$", "");
    }
}