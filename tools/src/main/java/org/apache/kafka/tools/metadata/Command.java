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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;

/**
 * A command for the Kafka metadata tool.
 */
public final class Command {
    private static final Logger log = LoggerFactory.getLogger(Command.class);

    /**
     * Create a new argument parser.
     *
     * @param addHelpAndExit    True if this parser should enable the help and exit
     *                          commands.
     */
    public static ArgumentParser createParser(boolean addHelpAndExit) {
        ArgumentParser parser = ArgumentParsers.
            newArgumentParser("metadata commands", addHelpAndExit).
                description("Commands for the Apache Kafka metadata tool");
        Subparsers subparsers = parser.addSubparsers().dest("command");
        Subparser catParser = subparsers.addParser("cat").
            help("Show the contents of metadata nodes.");
        catParser.addArgument("targets").
            nargs("+").
            help("The paths to show.");
        Subparser cdParser = subparsers.addParser("cd").
            help("Change to a new directory.");
        cdParser.addArgument("target").
            nargs("target").
            help("The directory to change to.");
        if (addHelpAndExit) {
            subparsers.addParser("exit").
                help("Exit the metadata shell.");
        }
        Subparser lsParser = subparsers.addParser("ls").
            help("List metadata nodes.");
        lsParser.addArgument("targets").
            nargs("+").
            help("The paths to list.");
        return parser;
    }

    public interface Handler {
        void run(PrintWriter writer, MetadataNodeManager manager);
    }

    final static ArgumentParser PARSER = createParser(true);

    public static Handler parseCommand(List<String> arguments) throws ArgumentParserException {
        Namespace namespace = PARSER.parseArgs(arguments.toArray(new String[0]));
        String command = namespace.get("command");
        if ("cat".equals(command)) {
            return new CatCommandHandler();
        } else if ("cd".equals(command)) {
            return new CdCommandHandler();
        } else if ("exit".equals(command)) {
            return new ExitCommandHandler();
        } else if ("help".equals(command)) {
            return new HelpCommandHandler();
        } else if ("ls".equals(command)) {
            return new LsCommandHandler();
        } else {
            return new ErroneousCommandHandler("Unknown command specified: " + command);
        }
    }
}
