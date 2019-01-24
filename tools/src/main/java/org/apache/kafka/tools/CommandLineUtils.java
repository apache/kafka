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
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.apache.kafka.common.utils.Exit;

/**
 * Helper functions for dealing with command line utilities
 */
public class CommandLineUtils {

    /**
     * Print usage and exit
     *
     * @param parser    parser used for command line arguments
     * @param message   message to print as an error before die
     */
    public static void printUsageAndDie(ArgumentParser parser, String message) {
        System.err.println(message);
        parser.printHelp();
        Exit.exit(1);
    }

    /**
     * Handle and print error information and exit
     *
     * @param parser    parser used for command line arguments
     * @param e exception to handle
     */
    public static void printErrorAndDie(ArgumentParser parser, ArgumentParserException e) {
        parser.handleError(e);
        Exit.exit(1);
    }
}
