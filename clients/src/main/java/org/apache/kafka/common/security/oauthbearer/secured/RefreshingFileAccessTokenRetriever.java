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

package org.apache.kafka.common.security.oauthbearer.secured;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.kafka.common.utils.Utils;

/**
 * <code>RefreshingFileAccessTokenRetriever</code> is an {@link AccessTokenRetriever} that will
 * watch a configured file for changes
 * ({@link LoginCallbackHandlerConfiguration#ACCESS_TOKEN_FILE_CONFIG}) and automatically
 * reload the contents, interpreting them as a JWT access key in the serialized form.
 *
 * @see AccessTokenRetriever
 * @see LoginCallbackHandlerConfiguration#ACCESS_TOKEN_FILE_CONFIG
 */

public class RefreshingFileAccessTokenRetriever extends DelegatedFileUpdate<String> implements AccessTokenRetriever {

    public RefreshingFileAccessTokenRetriever(Path accessTokenFile) {
        super(accessTokenFile);
    }

    @Override
    public String retrieve() throws IOException {
        String localDelegate = retrieveDelegate();

        if (localDelegate == null)
            throw new IOException("Access token delegate is null");

        return localDelegate;
    }

    @Override
    protected String createDelegate() throws IOException {
        return Utils.readFileAsString(path.toFile().getPath());
    }

}
