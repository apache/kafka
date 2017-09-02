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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;

/**
 * Base class for command line tools options
 */
public abstract class CommandOptions {

    protected final ArgumentParser parser;
    protected final Namespace ns;


    public CommandOptions(String command, String description, String[] args) {
        this.parser = ArgumentParsers
                .newArgumentParser(command)
                .defaultHelp(true)
                .description(description);

        prepareArgs();

        Namespace ns = null;
        try {
            ns = this.parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
        }
        this.ns = ns;
    }

    public boolean has(String dest) {
        return ns.get(dest) != null;
    }

    protected abstract void prepareArgs();

}
