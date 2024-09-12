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

package org.apache.kafka.shell.node.printer;

import org.apache.kafka.image.node.printer.MetadataNodePrinter;
import org.apache.kafka.image.node.printer.MetadataNodeRedactionCriteria;

import java.io.PrintWriter;


/**
 * Prints Kafka metadata shell nodes.
 */
public class ShellNodePrinter implements MetadataNodePrinter {
    private final PrintWriter writer;
    private int indentationLevel;

    public ShellNodePrinter(PrintWriter writer) {
        this.writer = writer;
    }

    String indentationString() {
        StringBuilder bld = new StringBuilder();
        for (int i = 0; i < indentationLevel; i++) {
            for (int j = 0; j < 2; j++) {
                bld.append(" ");
            }
        }
        return bld.toString();
    }

    @Override
    public MetadataNodeRedactionCriteria redactionCriteria() {
        return MetadataNodeRedactionCriteria.Disabled.INSTANCE;
    }

    @Override
    public void enterNode(String name) {
        writer.append(String.format("%s%s:%n", indentationString(), name));
        indentationLevel++;
    }

    @Override
    public void leaveNode() {
        indentationLevel--;
    }

    @Override
    public void output(String text) {
        writer.append(String.format("%s%s%n", indentationString(), text));
    }
}
