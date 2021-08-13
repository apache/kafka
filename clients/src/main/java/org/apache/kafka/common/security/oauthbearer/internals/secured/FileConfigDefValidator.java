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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import java.io.File;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class FileConfigDefValidator implements ConfigDef.Validator {

    private final boolean isRequired;

    public FileConfigDefValidator() {
        this(false);
    }

    public FileConfigDefValidator(boolean isRequired) {
        this.isRequired = isRequired;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value == null) {
            if (isRequired)
                throw new ConfigException(String.format("The OAuth configuration option %s is required", name));
            else
                return;
        }

        File file = new File(value.toString().trim());

        if (!file.exists())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a file (%s) that doesn't exist", name, value));

        if (!file.canRead())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a file (%s) that doesn't have read permission", name, value));
    }

}
