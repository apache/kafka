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

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * The Kafka metadata shell.
 */
public final class InteractiveShell implements AutoCloseable {
    static class MetadataShellCompleter implements Completer {
        private final MetadataNodeManager nodeManager;

        MetadataShellCompleter(MetadataNodeManager nodeManager) {
            this.nodeManager = nodeManager;
        }

        @Override
        public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
            if (line.words().size() == 0) {
                CommandUtils.completeCommand("", candidates);
            } else if (line.words().size() == 1) {
                CommandUtils.completeCommand(line.words().get(0), candidates);
            } else {
                Iterator<String> iter = line.words().iterator();
                String command = iter.next();
                List<String> nextWords = new ArrayList<>();
                while (iter.hasNext()) {
                    nextWords.add(iter.next());
                }
                Commands.Type type = Commands.TYPES.get(command);
                if (type == null) {
                    return;
                }
                try {
                    type.completeNext(nodeManager, nextWords, candidates);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private final MetadataNodeManager nodeManager;
    private final Terminal terminal;
    private final Parser parser;
    private final History history;
    private final MetadataShellCompleter completer;
    private final LineReader reader;

    public InteractiveShell(MetadataNodeManager nodeManager) throws IOException {
        this.nodeManager = nodeManager;
        TerminalBuilder builder = TerminalBuilder.builder().
            system(true).
            nativeSignals(true);
        this.terminal = builder.build();
        this.parser = new DefaultParser();
        this.history = new DefaultHistory();
        this.completer = new MetadataShellCompleter(nodeManager);
        this.reader = LineReaderBuilder.builder().
            terminal(terminal).
            parser(parser).
            history(history).
            completer(completer).
            option(LineReader.Option.AUTO_FRESH_LINE, false).
            build();
    }

    public void runMainLoop() throws Exception {
        terminal.writer().println("[ Kafka Metadata Shell ]");
        terminal.flush();
        Commands commands = new Commands(true);
        while (true) {
            try {
                reader.readLine(">> ");
                ParsedLine parsedLine = reader.getParsedLine();
                Commands.Handler handler = commands.parseCommand(parsedLine.words());
                handler.run(Optional.of(this), terminal.writer(), nodeManager);
                terminal.writer().flush();
            } catch (UserInterruptException eof) {
                // Handle the user pressing control-C.
                terminal.writer().println("^C");
            } catch (EndOfFileException eof) {
                return;
            }
        }
    }

    public int screenWidth() {
        return terminal.getWidth();
    }

    public Iterator<Entry<Integer, String>> history(int numEntriesToShow) {
        if (numEntriesToShow < 0) {
            numEntriesToShow = 0;
        }
        int last = history.last();
        if (numEntriesToShow > last + 1) {
            numEntriesToShow = last + 1;
        }
        int first = last - numEntriesToShow + 1;
        if (first < history.first()) {
            first = history.first();
        }
        return new HistoryIterator(first, last);
    }

    public class HistoryIterator implements  Iterator<Entry<Integer, String>> {
        private int index;
        private int last;

        HistoryIterator(int index, int last) {
            this.index = index;
            this.last = last;
        }

        @Override
        public boolean hasNext() {
            return index <= last;
        }

        @Override
        public Entry<Integer, String> next() {
            if (index > last) {
                throw new NoSuchElementException();
            }
            int p = index++;
            return new AbstractMap.SimpleImmutableEntry<>(p, history.get(p));
        }
    }

    @Override
    public void close() throws IOException {
        terminal.close();
    }
}
