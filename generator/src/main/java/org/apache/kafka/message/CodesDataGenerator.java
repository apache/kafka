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
package org.apache.kafka.message;

import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class CodesDataGenerator {

    private final HeaderGenerator headerGenerator;
    private final CodeBuffer buffer;
    private final NavigableMap<Long, String> values;

    CodesDataGenerator(String packageName) {
        this.headerGenerator = new HeaderGenerator(packageName);
        this.buffer = new CodeBuffer();
        this.values = new TreeMap<Long, String>();
    }

    void generate(CodesSpec codes) throws Exception {
        generateClass(
                codes.generatedClassName(),
                codes.valueType(),
                codes.codes());
        headerGenerator.generate();
    }

    private void generateClass(String className, FieldType type, List<CodeSpec> codes) {
        buffer.printf("public final class %s {%n", className);
        buffer.incrementIndent();
        generateConstants(type, codes);
        generateIsValidMethod(type);
        generateNameMethod(className, type, codes);
        generatePrivateConstructor(className);
        buffer.decrementIndent();
        buffer.printf("%n");
        buffer.printf("}%n");
    }

    private void generateConstants(FieldType type, List<CodeSpec> codes) {
        for (CodeSpec code : codes) {
            buffer.printf("%n");
            buffer.printf("public static final %s %s = %s;%n", javaType(type), code.name(), code.value());
            String existing = values.put(code.value(), code.name());
            if (existing != null) {
                throw new RuntimeException("Code " + existing + " and " + code.name() + " both use value " + code.value());
            }
        }
    }

    private void generatePrivateConstructor(String className) {
        buffer.printf("%n");
        buffer.printf("private %s() {%n", className);
        buffer.printf("}%n", className);
    }

    private void generateIsValidMethod(FieldType type) {
        buffer.printf("%n");
        buffer.printf("public static boolean isValid(%s v) {%n", javaType(type));
        buffer.incrementIndent();
        buffer.printf("return %s;%n", validExpr(null, values));
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateNameMethod(String className, FieldType type, List<CodeSpec> codes) {
        buffer.printf("%n");
        buffer.printf("public static String name(%s v) {%n", javaType(type));
        buffer.incrementIndent();
        buffer.printf("switch (v) {%n");
        buffer.incrementIndent();
        for (CodeSpec code : codes) {
            buffer.printf("case %s: return \"%s\";%n", code.name(), code.name());
        }
        buffer.printf("default: throw new IllegalArgumentException(\"Unknown %s: \" + v);%n", className);
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    public static String validExpr(String qualifier, NavigableMap<Long, String> values) {
        Map.Entry<Long, String> s = null;
        Map.Entry<Long, String> l = null;
        List<String> terms = new ArrayList<>();
        for (Map.Entry<Long, String> entry : values.entrySet()) {
            if (s == null) {
                l = s = entry;
            } else if (entry.getKey() == l.getKey() + 1) {
                l = entry;
            } else {
                terms.add(inRangeTerm(qualifier, s, l));
                l = s = entry;
            }
        }
        terms.add(inRangeTerm(qualifier, s, l));
        return String.join(" || ", terms);
    }

    private static String inRangeTerm(String qualifier, Map.Entry<Long, String> s, Map.Entry<Long, String> l) {
        if (s.getValue().equals(l.getValue())) {
            return String.format("v == %s", qual(qualifier, l.getValue()));
        } else {
            return String.format("%s <= v && v <= %s", qual(qualifier, s.getValue()), qual(qualifier, l.getValue()));
        }
    }

    private static Object qual(String qualifier, String value) {
        return qualifier == null || qualifier.isEmpty() ? value : qualifier + "." + value;
    }

    void write(Writer writer) throws Exception {
        headerGenerator.buffer().write(writer);
        buffer.write(writer);
    }

    private String javaType(FieldType type) {
        if (type instanceof FieldType.Int8FieldType) {
            return "byte";
        } else if (type instanceof FieldType.Int16FieldType) {
            return "short";
        } else if (type instanceof FieldType.Int32FieldType) {
            return "int";
        } else if (type instanceof FieldType.Int64FieldType) {
            return "long";
        } else {
            throw new RuntimeException("Unknown type type " + type);
        }
    }
}
