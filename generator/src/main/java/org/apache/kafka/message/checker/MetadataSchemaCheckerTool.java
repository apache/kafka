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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.kafka.message.checker.CheckerUtils.getDataFromGit;

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
            help("Verify that an evolution of a JSON file is valid.");
        evolutionVerifierParser.addArgument("--path1", "-1").
            required(true).
            help("The initial schema JSON path.");
        evolutionVerifierParser.addArgument("--path2", "-2").
            required(true).
            help("The final schema JSON path.");
        Subparser evolutionGitVerifierParser = subparsers.addParser("verify-evolution-git").
            help(" Verify that an evolution of a JSON file is valid using git.");
        evolutionGitVerifierParser.addArgument("--file", "-3").
            required(true).
            help("The edited JSON file");
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
                String path1 = namespace.getString("path1");
                String path2 = namespace.getString("path2");
                EvolutionVerifier verifier = new EvolutionVerifier(
                    CheckerUtils.readMessageSpecFromFile(path1),
                    CheckerUtils.readMessageSpecFromFile(path2));
                verifier.verify();
                writer.println("Successfully verified evolution of path1: " + path1 +
                        ", and path2: " + path2);
                break;
            }
            case "verify-evolution-git": {
                String filePath = "/metadata/src/main/resources/common/metadata/" + namespace.getString("file");
                Path rootKafkaDirectory = Paths.get("").toAbsolutePath();
                while (!Files.exists(rootKafkaDirectory.resolve(".git"))) {
                    rootKafkaDirectory = rootKafkaDirectory.getParent();
                    if (rootKafkaDirectory == null) {
                        throw new RuntimeException("Invalid directory, need to be within a Git repository");
                    }
                }
                String gitContent = getDataFromGit(filePath, rootKafkaDirectory, namespace.getString("ref"));
                EvolutionVerifier verifier = new EvolutionVerifier(
                        CheckerUtils.readMessageSpecFromFile(rootKafkaDirectory + filePath),
                        CheckerUtils.readMessageSpecFromString(gitContent));
                verifier.verify();
                writer.println("Successfully verified evolution of file: " + namespace.getString("file"));
                break;
            }
            default:
                throw new RuntimeException("Unknown command " + command);
        }
    }
}
