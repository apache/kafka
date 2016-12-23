/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.LoginType;

class JaasConfig extends Configuration {

    private final String loginContextName;
    private final AppConfigurationEntry[] appConfig;

    public JaasConfig(LoginType loginType, String jaasConfigParams) {
        StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(jaasConfigParams));
        tokenizer.slashSlashComments(true);
        tokenizer.slashStarComments(true);
        tokenizer.wordChars('-', '-');
        tokenizer.wordChars('_', '_');
        tokenizer.wordChars('$', '$');

        try {
            List<AppConfigurationEntry> entries = new ArrayList<>();
            while (tokenizer.nextToken() != StreamTokenizer.TT_EOF) {
                entries.add(parseAppConfigurationEntry(tokenizer));
            }
            if (entries.isEmpty())
                throw new IllegalArgumentException("Login module not specified in JAAS config");

            this.loginContextName = loginType.contextName();
            this.appConfig = entries.toArray(new AppConfigurationEntry[entries.size()]);

        } catch (IOException e) {
            throw new KafkaException("Unexpected exception while parsing JAAS config");
        }
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return this.loginContextName.equals(name) ? this.appConfig : null;
    }

    private LoginModuleControlFlag loginModuleControlFlag(String flag) {
        LoginModuleControlFlag controlFlag;
        switch (flag.toUpperCase(Locale.ROOT)) {
            case "REQUIRED":
                controlFlag = LoginModuleControlFlag.REQUIRED;
                break;
            case "REQUISITE":
                controlFlag = LoginModuleControlFlag.REQUISITE;
                break;
            case "SUFFICIENT":
                controlFlag = LoginModuleControlFlag.SUFFICIENT;
                break;
            case "OPTIONAL":
                controlFlag = LoginModuleControlFlag.OPTIONAL;
                break;
            default:
                throw new IllegalArgumentException("Invalid login module control flag '" + flag + "' in JAAS config");
        }
        return controlFlag;
    }

    private AppConfigurationEntry parseAppConfigurationEntry(StreamTokenizer tokenizer) throws IOException {
        String loginModule = tokenizer.sval;
        if (tokenizer.nextToken() == StreamTokenizer.TT_EOF)
            throw new IllegalArgumentException("Login module control flag not specified in JAAS config");
        LoginModuleControlFlag controlFlag = loginModuleControlFlag(tokenizer.sval);
        Map<String, String> options = new HashMap<>();
        while (tokenizer.nextToken() != StreamTokenizer.TT_EOF && tokenizer.ttype != ';') {
            String key = tokenizer.sval;
            if (tokenizer.nextToken() != '=' || tokenizer.nextToken() == StreamTokenizer.TT_EOF || tokenizer.sval == null)
                throw new IllegalArgumentException("Value not specified for key '" + key + "' in JAAS config");
            String value = tokenizer.sval;
            options.put(key, value);
        }
        if (tokenizer.ttype != ';')
            throw new IllegalArgumentException("JAAS config entry not terminated by semi-colon");
        return new AppConfigurationEntry(loginModule, controlFlag, options);
    }
}