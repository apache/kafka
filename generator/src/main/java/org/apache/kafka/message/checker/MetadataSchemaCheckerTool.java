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

package org.apache.kafka.message.checker;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import java.io.PrintStream;

import static org.apache.kafka.message.checker.CheckerUtils.readFileFromGitRef;

public class MetadataSchemaCheckerTool {
    public static void main(String[] args) throws Exception {
        try {
            run(args, System.out);
        } catch (HelpScreenException e) {
        }
    }

    public static void run(
        String[] args,
        PrintStream writer
    ) throws Exception {
        ArgumentParser argumentParser = ArgumentParsers.
            newArgumentParser("metadata-schema-checker").
            defaultHelp(true).
            description("The Kafka metadata schema checker tool.");
        Subparsers subparsers = argumentParser.addSubparsers().dest("command");
        Subparser parseParser = subparsers.addParser("parse").
            help("Verify that a JSON file can be parsed as a MessageSpec.");
        parseParser.addArgument("--path", "-p").
            required(true).
            help("The path to a schema JSON file.");
        Subparser evolutionVerifierParser = subparsers.addParser("verify-evolution").
            help("Verify that a schema JSON file is a valid evolution of a parent schema.");
        evolutionVerifierParser.addArgument("--path", "-1").
            required(true).
            help("The path to a schema JSON file.");
        evolutionVerifierParser.addArgument("--parent_path", "-2").
            required(true).
            help("The path to the parent schema JSON file.");
        Subparser evolutionGitVerifierParser = subparsers.addParser("verify-evolution-git").
            help("Verify that a schema JSON file is a valid evolution of your local git master branch.");
        evolutionGitVerifierParser.addArgument("--path", "-3").
            required(true).
            help("The path to your edited JSON file");
        evolutionGitVerifierParser.addArgument("--ref", "-4")
            .required(false)
            .setDefault("refs/heads/trunk")
            .help("Optional Git reference to be used for testing. Defaults to 'refs/heads/trunk' if not specified.");
        Namespace namespace;
        if (args.length == 0) {
            namespace = argumentParser.parseArgs(new String[] {"--help"});
        } else {
            namespace = argumentParser.parseArgs(args);
        }
        String command = namespace.getString("command");
        switch (command) {
            case "parse": {
                String path = namespace.getString("path");
                CheckerUtils.readMessageSpecFromFile(path);
                writer.println("Successfully parsed file as MessageSpec: " + path);
                break;
            }
            case "verify-evolution": {
                String child = namespace.getString("path");
                String parent = namespace.getString("parent_path");
                EvolutionVerifier verifier = new EvolutionVerifier(
                    CheckerUtils.readMessageSpecFromFile(parent),
                    CheckerUtils.readMessageSpecFromFile(child));
                verifier.verify();
                writer.println("Successfully verified evolution of path: " + child +
                        " from parent: " + parent);
                break;
            }
            case "verify-evolution-git": {
                String path = namespace.getString("path");
                String gitContent = readFileFromGitRef(path, namespace.getString("ref"));
                EvolutionVerifier verifier = new EvolutionVerifier(
                    CheckerUtils.readMessageSpecFromFile(path),
                    CheckerUtils.readMessageSpecFromString(gitContent));
                verifier.verify();
                writer.println("Successfully verified evolution of file: " + namespace.getString("path"));
                break;
            }
            default:
                throw new RuntimeException("Unknown command " + command);
        }
    }
}
