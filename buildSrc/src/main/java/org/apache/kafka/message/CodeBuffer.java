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

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;

public class CodeBuffer {
    private final ArrayList<String> lines;
    private int indent;

    public CodeBuffer() {
        this.lines = new ArrayList<>();
        this.indent = 0;
    }

    public void incrementIndent() {
        indent++;
    }

    public void decrementIndent() {
        indent--;
        if (indent < 0) {
            throw new RuntimeException("Indent < 0");
        }
    }

    public void printf(String format, Object... args) {
        lines.add(String.format(indentSpaces() + format, args));
    }

    public void write(Writer writer) throws IOException {
        for (String line : lines) {
            writer.write(line);
        }
    }

    public void write(CodeBuffer other) {
        for (String line : lines) {
            other.lines.add(other.indentSpaces() + line);
        }
    }

    private String indentSpaces() {
        StringBuilder bld = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            bld.append("    ");
        }
        return bld.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CodeBuffer)) {
            return false;
        }
        CodeBuffer o = (CodeBuffer) other;
        return lines.equals(o.lines);
    }

    @Override
    public int hashCode() {
        return lines.hashCode();
    }
}
