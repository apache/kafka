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

import org.apache.kafka.shell.command.CommandUtils;
import org.apache.kafka.shell.command.Commands;
import org.apache.kafka.shell.state.MetadataShellState;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
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
 * Handles running the Kafka metadata shell in interactive mode, where we accept input in real time.
 */
public final class InteractiveShell implements AutoCloseable {
    static class MetadataShellCompleter implements Completer {
        private final MetadataShellState state;

        MetadataShellCompleter(MetadataShellState state) {
            this.state = state;
        }

        @Override
        public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
            if (line.words().isEmpty()) {
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
                    type.completeNext(state, nextWords, candidates);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private final MetadataShellState state;
    private final Terminal terminal;
    private final History history;
    private final LineReader reader;

    public InteractiveShell(MetadataShellState state) throws IOException {
        this.state = state;
        TerminalBuilder builder = TerminalBuilder.builder().
            system(true).
            nativeSignals(true);
        this.terminal = builder.build();
        this.history = new DefaultHistory();
        this.reader = LineReaderBuilder.builder().
            terminal(terminal).
            parser(new DefaultParser()).
            history(history).
            completer(new MetadataShellCompleter(state)).
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
                handler.run(Optional.of(this), terminal.writer(), state);
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
        private final int last;
        private int index;

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
