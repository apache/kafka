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

import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

public class ConfigCommandOptionsTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testBootstrapServersRequired() throws Exception {
        ConfigCommand.ConfigCommandOptions opts = new ConfigCommand.ConfigCommandOptions();
        expectedException.expect(ArgumentParserException.class);
        expectedException.expectMessage("argument --bootstrap-servers is required");
        opts.parseArgs(new String[0]);
    }

    @Test
    public void testEntityTypeRequired() throws Exception {
        ConfigCommand.ConfigCommandOptions opts = new ConfigCommand.ConfigCommandOptions();
        expectedException.expect(ArgumentParserException.class);
        expectedException.expectMessage("argument --entity-type is required");
        opts.parseArgs(Arrays.asList("--bootstrap-servers", "localhost:9092").toArray(new String[]{}));
    }

    @Test
    public void testAlterOrDescribeRequired() throws Exception {
        ConfigCommand.ConfigCommandOptions opts = new ConfigCommand.ConfigCommandOptions();
        expectedException.expect(ArgumentParserException.class);
        expectedException.expectMessage("one of the arguments --describe --alter is required");
        opts.parseArgs(Arrays.asList("--bootstrap-servers", "localhost:9092", "--entity-type", "topics").toArray(new String[]{}));
    }

    @Test
    public void testAlterOrDescribeExclusion() throws Exception {
        ConfigCommand.ConfigCommandOptions opts = new ConfigCommand.ConfigCommandOptions();
        expectedException.expect(ArgumentParserException.class);
        expectedException.expectMessage("argument --describe: not allowed with argument --alter");
        opts.parseArgs(Arrays.asList("--bootstrap-servers", "localhost:9092", "--entity-type", "topics", "--alter", "--describe").toArray(new String[]{}));
    }

    @Test
    public void testAlterAndNoAddOrDelete() throws Exception {
        ConfigCommand.ConfigCommandOptions opts = new ConfigCommand.ConfigCommandOptions();
        expectedException.expect(ArgumentParserException.class);
        expectedException.expectMessage("If --alter specified, you must also specify --add-config or --delete-config");
        opts.parseArgs(Arrays.asList("--bootstrap-servers", "localhost:9092", "--entity-type", "topics", "--alter").toArray(new String[]{}));
    }

    @Test
    public void testEntityNameWithAlterRequired() throws Exception {
        ConfigCommand.ConfigCommandOptions opts = new ConfigCommand.ConfigCommandOptions();
        expectedException.expect(ArgumentParserException.class);
        expectedException.expectMessage("If --alter specified, you must also specify --entity-name");
        opts.parseArgs(Arrays.asList("--bootstrap-servers", "localhost:9092", "--entity-type", "topics", "--alter", "--add-config", "\"K=V\"").toArray(new String[]{}));
    }

    @Test
    public void testDescribeWithAddOrDeleteFails() throws Exception {
        ConfigCommand.ConfigCommandOptions opts = new ConfigCommand.ConfigCommandOptions();
        expectedException.expect(ArgumentParserException.class);
        expectedException.expectMessage("If --describe specified, --add-config and --delete-config are not valid options");
        opts.parseArgs(Arrays.asList("--bootstrap-servers", "localhost:9092", "--entity-type", "topics", "--describe", "--add-config", "\"K=V\"").toArray(new String[]{}));

        expectedException.expect(ArgumentParserException.class);
        expectedException.expectMessage("If --describe specified, --add-config and --delete-config are not valid options");
        opts.parseArgs(Arrays.asList("--bootstrap-servers", "localhost:9092", "--entity-type", "topics", "--describe", "--delete-config", "\"K\"").toArray(new String[]{}));
    }

    @Test
    public void testInvalidEntityType() throws Exception {
        ConfigCommand.ConfigCommandOptions opts = new ConfigCommand.ConfigCommandOptions();
        expectedException.expect(ArgumentParserException.class);
        expectedException.expectMessage("argument --entity-type: could not convert 'invalid_type'");
        opts.parseArgs(Arrays.asList("--bootstrap-servers", "localhost:9092", "--entity-type", "invalid_type", "--describe").toArray(new String[]{}));
    }

    @Test
    public void testBootstrapServersAndConfigPropertiesSpecified() throws Exception {
        ConfigCommand.ConfigCommandOptions opts = new ConfigCommand.ConfigCommandOptions();
        expectedException.expect(ArgumentParserException.class);
        expectedException.expectMessage("argument --config.properties: not allowed with argument --bootstrap-servers");
        opts.parseArgs(Arrays.asList("--bootstrap-servers", "localhost:9092", "--config.properties", "invalid_file").toArray(new String[]{}));
    }
}
