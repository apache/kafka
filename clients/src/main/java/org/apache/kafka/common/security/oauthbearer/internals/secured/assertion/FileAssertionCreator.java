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
package org.apache.kafka.common.security.oauthbearer.internals.secured.assertion;

import org.apache.kafka.common.security.oauthbearer.internals.secured.CachedFile;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

import static org.apache.kafka.common.security.oauthbearer.internals.secured.CachedFile.RefreshPolicy.lastModifiedPolicy;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.CachedFile.STRING_JSON_VALIDATING_TRANSFORMER;

/**
 * An {@link AssertionCreator} which takes a file from which the pre-created assertion is loaded and returned.
 * If the file changes on disk, it will be reloaded in memory without needing to restart the client/application.
 */
public class FileAssertionCreator implements AssertionCreator {

    private final CachedFile<String> assertionFile;

    public FileAssertionCreator(File assertionFile) {
        this.assertionFile = new CachedFile<>(assertionFile, STRING_JSON_VALIDATING_TRANSFORMER, lastModifiedPolicy());
    }

    @Override
    public String create(AssertionJwtTemplate ignored) throws GeneralSecurityException, IOException {
        return assertionFile.transformed();
    }
}
