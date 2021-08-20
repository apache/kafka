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
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

/**
 * An implementation of {@link org.apache.kafka.common.config.ConfigDef.Validator} that, if a value
 * is supplied, is assumed to:
 *
 * <li>
 *     <ul>exist</ul>
 *     <ul>have read permission</ul>
 *     <ul>point to a file</ul>
 * </li>
 *
 * If the value is null or an empty string, it is assumed to be an "empty" value and thus ignored.
 * Any whitespace is trimmed off of the beginning and end.
 */

public class FileConfigDefValidator implements Validator {

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null || value.toString().trim().isEmpty())
            return;

        File file = new File(value.toString().trim());

        if (!file.exists())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a file (%s) that doesn't exist", name, value));

        if (!file.canRead())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a file (%s) that doesn't have read permission", name, value));

        if (file.isDirectory())
            throw new ConfigException(String.format("The OAuth configuration option %s references a directory (%s), not a file", name, value));
    }

}
