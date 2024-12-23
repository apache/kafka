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

package org.apache.kafka.shell.command;

import org.apache.kafka.image.node.MetadataNode;
import org.apache.kafka.shell.InteractiveShell;
import org.apache.kafka.shell.glob.GlobVisitor;
import org.apache.kafka.shell.glob.GlobVisitor.MetadataNodeInfo;
import org.apache.kafka.shell.state.MetadataShellState;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.jline.reader.Candidate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Implements the ls command.
 */
public final class LsCommandHandler implements Commands.Handler {
    private static final Logger log = LoggerFactory.getLogger(LsCommandHandler.class);

    public static final Commands.Type TYPE = new LsCommandType();

    public static class LsCommandType implements Commands.Type {
        private LsCommandType() {
        }

        @Override
        public String name() {
            return "ls";
        }

        @Override
        public String description() {
            return "List metadata nodes.";
        }

        @Override
        public boolean shellOnly() {
            return false;
        }

        @Override
        public void addArguments(ArgumentParser parser) {
            parser.addArgument("targets").
                nargs("*").
                help("The metadata node paths to list.");
        }

        @Override
        public Commands.Handler createHandler(Namespace namespace) {
            return new LsCommandHandler(namespace.getList("targets"));
        }

        @Override
        public void completeNext(
            MetadataShellState state,
            List<String> nextWords,
            List<Candidate> candidates
        ) throws Exception {
            CommandUtils.completePath(state, nextWords.get(nextWords.size() - 1), candidates);
        }
    }

    private final List<String> targets;

    public LsCommandHandler(List<String> targets) {
        this.targets = targets;
    }

    static class TargetDirectory {
        private final String name;
        private final List<String> children;

        TargetDirectory(String name, List<String> children) {
            this.name = name;
            this.children = children;
        }
    }

    @Override
    public void run(
        Optional<InteractiveShell> shell,
        PrintWriter writer,
        MetadataShellState state
    ) throws Exception {
        List<String> targetFiles = new ArrayList<>();
        List<TargetDirectory> targetDirectories = new ArrayList<>();
        for (String target : CommandUtils.getEffectivePaths(targets)) {
            state.visit(new GlobVisitor(target, entryOption -> {
                if (entryOption.isPresent()) {
                    MetadataNodeInfo info = entryOption.get();
                    MetadataNode node = info.node();
                    if (node.isDirectory()) {
                        List<String> children = new ArrayList<>(node.childNames());
                        children.sort(String::compareTo);
                        targetDirectories.add(
                            new TargetDirectory(info.lastPathComponent(), children));
                    } else {
                        targetFiles.add(info.lastPathComponent());
                    }
                } else {
                    writer.println("ls: " + target + ": no such file or directory.");
                }
            }));
        }
        OptionalInt screenWidth = shell.map(interactiveShell -> OptionalInt.of(interactiveShell.screenWidth())).orElseGet(OptionalInt::empty);
        log.trace("LS : targetFiles = {}, targetDirectories = {}, screenWidth = {}",
            targetFiles, targetDirectories, screenWidth);
        printTargets(writer, screenWidth, targetFiles, targetDirectories);
    }

    static void printTargets(PrintWriter writer,
                             OptionalInt screenWidth,
                             List<String> targetFiles,
                             List<TargetDirectory> targetDirectories) {
        printEntries(writer, "", screenWidth, targetFiles);
        boolean needIntro = !targetFiles.isEmpty() || targetDirectories.size() > 1;
        boolean firstIntro = targetFiles.isEmpty();
        for (TargetDirectory targetDirectory : targetDirectories) {
            String intro = "";
            if (needIntro) {
                if (!firstIntro) {
                    intro = intro + String.format("%n");
                }
                intro = intro + targetDirectory.name + ":";
                firstIntro = false;
            }
            log.trace("LS : targetDirectory name = {}, children = {}",
                targetDirectory.name, targetDirectory.children);
            printEntries(writer, intro, screenWidth, targetDirectory.children);
        }
    }

    static void printEntries(PrintWriter writer,
                             String intro,
                             OptionalInt screenWidth,
                             List<String> entries) {
        if (entries.isEmpty()) {
            return;
        }
        if (!intro.isEmpty()) {
            writer.println(intro);
        }
        ColumnSchema columnSchema = calculateColumnSchema(screenWidth, entries);
        int numColumns = columnSchema.numColumns();
        int numLines = (entries.size() + numColumns - 1) / numColumns;
        for (int line = 0; line < numLines; line++) {
            StringBuilder output = new StringBuilder();
            for (int column = 0; column < numColumns; column++) {
                int entryIndex = line + (column * columnSchema.entriesPerColumn());
                if (entryIndex < entries.size()) {
                    String entry = entries.get(entryIndex);
                    output.append(entry);
                    if (column < numColumns - 1) {
                        int width = columnSchema.columnWidth(column);
                        for (int i = 0; i < width - entry.length(); i++) {
                            output.append(" ");
                        }
                    }
                }
            }
            writer.println(output);
        }
    }

    static ColumnSchema calculateColumnSchema(OptionalInt screenWidth,
                                              List<String> entries) {
        if (screenWidth.isEmpty()) {
            return new ColumnSchema(1, entries.size());
        }
        int maxColumns = screenWidth.getAsInt() / 4;
        if (maxColumns <= 1) {
            return new ColumnSchema(1, entries.size());
        }
        ColumnSchema[] schemas = new ColumnSchema[maxColumns];
        for (int numColumns = 1; numColumns <= maxColumns; numColumns++) {
            schemas[numColumns - 1] = new ColumnSchema(numColumns,
                (entries.size() + numColumns - 1) / numColumns);
        }
        for (int i = 0; i < entries.size(); i++) {
            String entry = entries.get(i);
            for (ColumnSchema schema : schemas) {
                schema.process(i, entry);
            }
        }
        for (int s = schemas.length - 1; s > 0; s--) {
            ColumnSchema schema = schemas[s];
            if (schema.columnWidths[schema.columnWidths.length - 1] != 0 &&
                    schema.totalWidth() <= screenWidth.getAsInt()) {
                return schema;
            }
        }
        return schemas[0];
    }

    static class ColumnSchema {
        private final int[] columnWidths;
        private final int entriesPerColumn;

        ColumnSchema(int numColumns, int entriesPerColumn) {
            this.columnWidths = new int[numColumns];
            this.entriesPerColumn = entriesPerColumn;
        }

        ColumnSchema setColumnWidths(Integer... widths) {
            for (int i = 0; i < widths.length; i++) {
                columnWidths[i] = widths[i];
            }
            return this;
        }

        void process(int entryIndex, String output) {
            int columnIndex = entryIndex / entriesPerColumn;
            columnWidths[columnIndex] = Math.max(
                columnWidths[columnIndex], output.length() + 2);
        }

        int totalWidth() {
            int total = 0;
            for (int columnWidth : columnWidths) {
                total += columnWidth;
            }
            return total;
        }

        int numColumns() {
            return columnWidths.length;
        }

        int columnWidth(int columnIndex) {
            return columnWidths[columnIndex];
        }

        int entriesPerColumn() {
            return entriesPerColumn;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(columnWidths), entriesPerColumn);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ColumnSchema)) return false;
            ColumnSchema other = (ColumnSchema) o;
            if (entriesPerColumn != other.entriesPerColumn) return false;
            return Arrays.equals(columnWidths, other.columnWidths);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder("ColumnSchema(columnWidths=[");
            String prefix = "";
            for (int columnWidth : columnWidths) {
                bld.append(prefix);
                bld.append(columnWidth);
                prefix = ", ";
            }
            bld.append("], entriesPerColumn=").append(entriesPerColumn).append(")");
            return bld.toString();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(targets);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LsCommandHandler)) return false;
        LsCommandHandler o = (LsCommandHandler) other;
        return Objects.equals(o.targets, targets);
    }
}
