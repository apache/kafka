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
package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.config.types.Password;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

class SecurityStore {
    private final String type;
    private final String path;

    private final Password password;

    SecurityStore(String type, String path, Password password) {
        this.type = type == null ? KeyStore.getDefaultType() : type;
        this.path = path;
        this.password = password;
    }

    Password getPassword() {
        return password;
    }

    KeyStore load() throws GeneralSecurityException, IOException {
        FileInputStream in = null;
        try {
            KeyStore ks = KeyStore.getInstance(type);
            in = new FileInputStream(path);
            // If a password is not set access to the truststore is still available, but integrity checking is disabled.
            char[] passwordChars = password != null ? password.value().toCharArray() : null;
            ks.load(in, passwordChars);
            return ks;
        } finally {
            if (in != null) in.close();
        }
    }

    long getLastModified() {
        File storeFile = new File(path);
        return storeFile.lastModified();
    }
}
