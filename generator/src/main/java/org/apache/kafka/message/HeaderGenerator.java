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

import java.util.TreeSet;

/**
 * The Kafka header generator.
 */
public final class HeaderGenerator {
    private static final String[] HEADER = new String[] {
        "/*",
        " * Licensed to the Apache Software Foundation (ASF) under one or more",
        " * contributor license agreements. See the NOTICE file distributed with",
        " * this work for additional information regarding copyright ownership.",
        " * The ASF licenses this file to You under the Apache License, Version 2.0",
        " * (the \"License\"); you may not use this file except in compliance with",
        " * the License. You may obtain a copy of the License at",
        " *",
        " *    http://www.apache.org/licenses/LICENSE-2.0",
        " *",
        " * Unless required by applicable law or agreed to in writing, software",
        " * distributed under the License is distributed on an \"AS IS\" BASIS,",
        " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
        " * See the License for the specific language governing permissions and",
        " * limitations under the License.",
        " */",
        "",
        "// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.",
        ""
    };

    static final String PACKAGE = "org.apache.kafka.common.message";

    private final CodeBuffer buffer;

    private final TreeSet<String> imports;

    public HeaderGenerator() {
        this.buffer = new CodeBuffer();
        this.imports = new TreeSet<>();
    }

    public void addImport(String newImport) {
        this.imports.add(newImport);
    }

    public void generate() {
        for (int i = 0; i < HEADER.length; i++) {
            buffer.printf("%s%n", HEADER[i]);
        }
        buffer.printf("package %s;%n", PACKAGE);
        buffer.printf("%n");
        for (String newImport : imports) {
            buffer.printf("import %s;%n", newImport);
        }
        buffer.printf("%n");
    }

    public CodeBuffer buffer() {
        return buffer;
    }
}
