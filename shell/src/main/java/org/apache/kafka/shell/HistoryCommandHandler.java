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

package org.apache.kafka.shell;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.jline.reader.Candidate;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Implements the history command.
 */
public final class HistoryCommandHandler implements Commands.Handler {
    public final static Commands.Type TYPE = new HistoryCommandType();

    public static class HistoryCommandType implements Commands.Type {
        private HistoryCommandType() {
        }

        @Override
        public String name() {
            return "history";
        }

        @Override
        public String description() {
            return "Print command history.";
        }

        @Override
        public boolean shellOnly() {
            return true;
        }

        @Override
        public void addArguments(ArgumentParser parser) {
            parser.addArgument("numEntriesToShow").
                nargs("?").
                type(Integer.class).
                help("The number of entries to show.");
        }

        @Override
        public Commands.Handler createHandler(Namespace namespace) {
            Integer numEntriesToShow = namespace.getInt("numEntriesToShow");
            return new HistoryCommandHandler(numEntriesToShow == null ?
                Integer.MAX_VALUE : numEntriesToShow);
        }

        @Override
        public void completeNext(MetadataNodeManager nodeManager, List<String> nextWords,
                                 List<Candidate> candidates) throws Exception {
            // nothing to do
        }
    }

    private final int numEntriesToShow;

    public HistoryCommandHandler(int numEntriesToShow) {
        this.numEntriesToShow = numEntriesToShow;
    }

    @Override
    public void run(Optional<InteractiveShell> shell,
                    PrintWriter writer,
                    MetadataNodeManager manager) throws Exception {
        if (!shell.isPresent()) {
            throw new RuntimeException("The history command requires a shell.");
        }
        Iterator<Map.Entry<Integer, String>> iter = shell.get().history(numEntriesToShow);
        while (iter.hasNext()) {
            Map.Entry<Integer, String> entry = iter.next();
            writer.printf("% 5d  %s%n", entry.getKey(), entry.getValue());
        }
    }

    @Override
    public int hashCode() {
        return numEntriesToShow;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof HistoryCommandHandler)) return false;
        HistoryCommandHandler o = (HistoryCommandHandler) other;
        return o.numEntriesToShow == numEntriesToShow;
    }
}
