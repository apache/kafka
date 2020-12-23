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

package org.apache.kafka.tools.metadata;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import net.sourceforge.argparse4j.internal.HelpScreenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

/**
 * A command for the Kafka metadata tool.
 */
public final class Command {
    private static final Logger log = LoggerFactory.getLogger(Command.class);

    /**
     * Create a new argument parser.
     *
     * @param addShellCommands  Whether we should add the commands that only make sense
     *                          in interactive shell mode.
     */
    public static ArgumentParser createParser(boolean addShellCommands) {
        ArgumentParser parser = ArgumentParsers.
            newArgumentParser("", false).
                description("Commands for the Apache Kafka metadata tool");
        Subparsers subparsers = parser.addSubparsers().dest("command");
        Subparser catParser = subparsers.addParser("cat").
            help("Show the contents of metadata nodes.");
        catParser.addArgument("targets").
            nargs("+").
            help("The metadata nodes to display.");
        Subparser cdParser = subparsers.addParser("cd").
            help("Set the current working directory.");
        cdParser.addArgument("target").
            nargs("?").
            help("The metadata node directory to change to.");
        if (addShellCommands) {
            subparsers.addParser("exit").
                help("Exit the metadata shell.");
        }
        if (addShellCommands) {
            Subparser historyParser = subparsers.addParser("history").
                help("Print command history.");
            historyParser.addArgument("numEntriesToShow").
                nargs("?").
                type(Integer.class).
                help("The number of entries to show.");
        }
        if (addShellCommands) {
            subparsers.addParser("help").
                help("Display this help message.");
        }
        Subparser lsParser = subparsers.addParser("ls").
            help("List metadata nodes.");
        lsParser.addArgument("targets").
            nargs("*").
            help("The metadata node paths to list.");
        subparsers.addParser("pwd").
            help("Print the current working directory.");
        return parser;
    }

    public interface Handler {
        void run(Optional<MetadataShell> shell,
                 PrintWriter writer,
                 MetadataNodeManager manager) throws Exception;
    }

    final static ArgumentParser PARSER = createParser(true);

    public static Handler parseCommand(List<String> arguments) {
        Namespace namespace = null;
        try {
            namespace = PARSER.parseArgs(arguments.toArray(new String[0]));
        } catch (HelpScreenException e) {
            return new NoOpCommandHandler();
        } catch (ArgumentParserException e) {
            return new ErroneousCommandHandler(e.getMessage());
        }
        String command = namespace.get("command");
        if ("cat".equals(command)) {
            return new CatCommandHandler(namespace.getList("targets"));
        } else if ("cd".equals(command)) {
            return new CdCommandHandler(Optional.ofNullable(namespace.getString("target")));
        } else if ("exit".equals(command)) {
            return new ExitCommandHandler();
        } else if ("help".equals(command)) {
            return new HelpCommandHandler();
        } else if ("history".equals(command)) {
            Integer numEntriesToShow = namespace.getInt("numEntriesToShow");
            return new HistoryCommandHandler(numEntriesToShow == null ?
                Integer.MAX_VALUE : numEntriesToShow);
        } else if ("ls".equals(command)) {
            return new LsCommandHandler(namespace.getList("targets"));
        } else if ("pwd".equals(command)) {
            return new PwdCommandHandler();
        } else {
            return new ErroneousCommandHandler("Unknown command specified: " + command);
        }
    }
}