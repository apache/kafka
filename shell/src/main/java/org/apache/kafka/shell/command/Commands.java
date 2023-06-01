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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import net.sourceforge.argparse4j.internal.HelpScreenException;
import org.apache.kafka.shell.InteractiveShell;
import org.apache.kafka.shell.state.MetadataShellState;
import org.jline.reader.Candidate;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The commands for the Kafka metadata tool.
 */
public final class Commands {
    /**
     * A map from command names to command types.
     */
    public static final NavigableMap<String, Type> TYPES;

    static {
        TreeMap<String, Type> typesMap = new TreeMap<>();
        for (Type type : Arrays.asList(
            CatCommandHandler.TYPE,
            CdCommandHandler.TYPE,
            ExitCommandHandler.TYPE,
            FindCommandHandler.TYPE,
            HelpCommandHandler.TYPE,
            HistoryCommandHandler.TYPE,
            LsCommandHandler.TYPE,
            ManCommandHandler.TYPE,
            PwdCommandHandler.TYPE,
            TreeCommandHandler.TYPE
        )) {
            typesMap.put(type.name(), type);
        }
        TYPES = Collections.unmodifiableNavigableMap(typesMap);
    }

    /**
     * Command handler objects are instantiated with specific arguments to
     * execute commands.
     */
    public interface Handler {
        void run(
            Optional<InteractiveShell> shell,
            PrintWriter writer,
            MetadataShellState state
        ) throws Exception;
    }

    /**
     * An object which describes a type of command handler. This includes
     * information like its name, help text, and whether it should be accessible
     * from non-interactive mode.
     */
    public interface Type {
        String name();
        String description();
        boolean shellOnly();
        void addArguments(ArgumentParser parser);
        Handler createHandler(Namespace namespace);
        void completeNext(
            MetadataShellState nodeManager,
            List<String> nextWords,
            List<Candidate> candidates
        ) throws Exception;
    }

    private final ArgumentParser parser;

    /**
     * Create the commands instance.
     *
     * @param addShellCommands  True if we should include the shell-only commands.
     */
    public Commands(boolean addShellCommands) {
        this.parser = ArgumentParsers.newArgumentParser("", false);
        Subparsers subparsers = this.parser.addSubparsers().dest("command");
        for (Type type : TYPES.values()) {
            if (addShellCommands || !type.shellOnly()) {
                Subparser subParser = subparsers.addParser(type.name());
                subParser.help(type.description());
                type.addArguments(subParser);
            }
        }
    }

    ArgumentParser parser() {
        return parser;
    }

    /**
     * Handle the given command.
     *
     * In general this function should not throw exceptions. Instead, it should
     * return ErroneousCommandHandler if the input was invalid.
     *
     * @param arguments     The command line arguments.
     * @return              The command handler.
     */
    public Handler parseCommand(List<String> arguments) {
        List<String> trimmedArguments = new ArrayList<>(arguments);
        while (true) {
            if (trimmedArguments.isEmpty()) {
                return new NoOpCommandHandler();
            }
            String last = trimmedArguments.get(trimmedArguments.size() - 1);
            if (!last.isEmpty()) {
                break;
            }
            trimmedArguments.remove(trimmedArguments.size() - 1);
        }
        Namespace namespace;
        try {
            namespace = parser.parseArgs(trimmedArguments.toArray(new String[0]));
        } catch (HelpScreenException e) {
            return new NoOpCommandHandler();
        } catch (ArgumentParserException e) {
            return new ErroneousCommandHandler(e.getMessage());
        }
        String command = namespace.get("command");
        if (!command.equals(trimmedArguments.get(0))) {
            return new ErroneousCommandHandler("invalid choice: '" +
                trimmedArguments.get(0) + "': did you mean '" + command + "'?");
        }
        Type type = TYPES.get(command);
        if (type == null) {
            return new ErroneousCommandHandler("Unknown command specified: " + command);
        } else {
            return type.createHandler(namespace);
        }
    }
}
