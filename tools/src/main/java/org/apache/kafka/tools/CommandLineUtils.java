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

package org.apache.kafka.tools;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import org.apache.kafka.common.utils.Exit;

import java.io.IOException;
import java.util.List;

/**
 * Helper functions for dealing with command line utilities
 */
public class CommandLineUtils {

    /**
     * Print usage and exit
     *
     * @param parser    parser used for command line arguments
     * @param message   message to print as an error before die
     * @throws IOException
     */
    public static void printUsageAndDie(ArgumentParser parser, String message) throws IOException {
        System.err.println(message);
        parser.printHelp();
        Exit.exit(1);
    }

    /**
     * Check that all the listed options are present
     *
     * @param parser    parser used for command line arguments
     * @param options   options specified as arguments on the command line
     * @param required  required options on the command line
     * @throws IOException
     */
    public static void checkRequiredArgs(ArgumentParser parser, CommandOptions options, List<String> required) throws IOException {
        for (String arg: required) {
            if (!options.has(arg))
                printUsageAndDie(parser, "Missing required argument \"" + arg + "\"");
        }
    }

    /**
     * Check that none of the listed options are present
     *
     * @param parser    parser used for command line arguments
     * @param options   options specified as arguments on the command line
     * @param usedOption    option for which check invalid options
     * @param invalidOptions    invalid options to check against the specified used option
     * @throws IOException
     */
    public static void checkInvalidArgs(ArgumentParser parser, CommandOptions options, String usedOption, List<String> invalidOptions) throws IOException {
        if (options.has(usedOption)) {
            for (String arg: invalidOptions) {
                if (options.has(arg))
                    printUsageAndDie(parser, "Option \"" + usedOption + "\" can't be used with option\"" + arg + "\"");
            }
        }
    }
}
