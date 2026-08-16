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
package org.apache.kafka.maven;

import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Opcodes;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Catches drift between the Mojo's {@code @Parameter} fields and the hand-maintained
 * {@code plugin.xml} descriptor. Adding a parameter to the Mojo without editing the descriptor
 * silently makes it non-configurable; this test fires the moment the two sides disagree.
 *
 * <p>If this fails, update {@code src/main/resources/META-INF/maven/plugin.xml} to add or remove
 * the listed parameter name(s) so the two stay in lockstep.
 */
class PluginXmlParityTest {

    @Test
    void everyParameterFieldHasAMatchingDescriptorEntry() throws Exception {
        Set<String> mojoParams = parameterFieldsOf(KafkaInternalApiCheckerMojo.class);
        Set<String> xmlParams = parametersDeclaredInPluginXml();

        assertEquals(new TreeSet<>(mojoParams), new TreeSet<>(xmlParams),
                "Mojo @Parameter fields and plugin.xml <parameter> entries are out of sync. "
                + "Mojo: " + new TreeSet<>(mojoParams) + ", plugin.xml: " + new TreeSet<>(xmlParams)
                + ". Edit src/main/resources/META-INF/maven/plugin.xml to add/remove entries.");

        // And the <configuration> block must carry an entry for each parameter too, otherwise the
        // parameter is declared but not wired to its property/default-value.
        Set<String> xmlConfig = configurationEntriesInPluginXml();
        assertTrue(xmlConfig.containsAll(mojoParams),
                "plugin.xml <configuration> is missing entries for: "
                        + diff(mojoParams, xmlConfig));
    }

    /**
     * Maven's {@code @Parameter} carries {@code @Retention(CLASS)}, so reflection on the loaded
     * class returns zero matches. Read the .class file directly via ASM, whose visitor pipeline
     * surfaces both CLASS- and RUNTIME-retained annotations.
     */
    private static Set<String> parameterFieldsOf(Class<?> mojo) throws Exception {
        Set<String> names = new LinkedHashSet<>();
        String resourcePath = mojo.getName().replace('.', '/') + ".class";
        try (InputStream in = mojo.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) throw new IllegalStateException("Mojo .class not found: " + resourcePath);
            new ClassReader(in).accept(new ClassVisitor(Opcodes.ASM9) {
                @Override
                public FieldVisitor visitField(int access, String name, String descriptor,
                                               String signature, Object value) {
                    return new FieldVisitor(Opcodes.ASM9) {
                        @Override
                        public org.objectweb.asm.AnnotationVisitor visitAnnotation(String desc, boolean visible) {
                            if ("Lorg/apache/maven/plugins/annotations/Parameter;".equals(desc)) {
                                names.add(name);
                            }
                            return null;
                        }
                    };
                }
            }, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
        }
        return names;
    }

    private static Set<String> parametersDeclaredInPluginXml() throws Exception {
        Document doc = loadPluginXml();
        NodeList params = doc.getElementsByTagName("parameter");
        Set<String> names = new LinkedHashSet<>();
        for (int i = 0; i < params.getLength(); i++) {
            Node n = params.item(i);
            // Skip <parameter> elements that aren't a direct child of <parameters> — e.g. nested
            // tag names that happen to collide. plugin.xml's only "parameter" nodes are inside
            // <mojo><parameters>, so this guard is defensive.
            if (!"parameters".equals(n.getParentNode().getNodeName())) continue;
            NodeList nameNodes = ((Element) n).getElementsByTagName("name");
            if (nameNodes.getLength() > 0) {
                names.add(nameNodes.item(0).getTextContent().trim());
            }
        }
        return names;
    }

    private static Set<String> configurationEntriesInPluginXml() throws Exception {
        Document doc = loadPluginXml();
        NodeList configs = doc.getElementsByTagName("configuration");
        Set<String> names = new LinkedHashSet<>();
        for (int i = 0; i < configs.getLength(); i++) {
            NodeList children = configs.item(i).getChildNodes();
            for (int j = 0; j < children.getLength(); j++) {
                Node child = children.item(j);
                if (child.getNodeType() == Node.ELEMENT_NODE) {
                    names.add(child.getNodeName());
                }
            }
        }
        return names;
    }

    private static Document loadPluginXml() throws Exception {
        File f = new File("src/main/resources/META-INF/maven/plugin.xml");
        assertTrue(f.exists(), "plugin.xml not found at " + f.getAbsolutePath()
                + " — run this test from the :maven-plugin project directory");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        // Don't try to fetch the DTD over the network — Maven's plugin descriptor schema isn't
        // hosted at a URL the test runner can reach in CI.
        dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        return dbf.newDocumentBuilder().parse(f);
    }

    private static Set<String> diff(Set<String> required, Set<String> have) {
        Set<String> missing = new TreeSet<>(required);
        missing.removeAll(have);
        return missing;
    }
}
