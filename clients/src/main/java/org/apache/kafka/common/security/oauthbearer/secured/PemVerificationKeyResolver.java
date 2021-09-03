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
import java.security.Key;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.kafka.common.security.oauthbearer.secured.FileWatchService.FileWatchCallback;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.jose4j.lang.UnresolvableKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PemVerificationKeyResolver implements CloseableVerificationKeyResolver {

    private static final Logger log = LoggerFactory.getLogger(PemVerificationKeyResolver.class);

    private final Path pemDirectory;

    private final PemVerificationKeyResolverFactory factory;

    private final ReentrantReadWriteLock lock;

    private FileWatchService fileWatchService;

    private VerificationKeyResolver delegate;

    public PemVerificationKeyResolver(Path pemDirectory) {
        this.pemDirectory = pemDirectory.toAbsolutePath();
        this.factory = new PemVerificationKeyResolverFactory(pemDirectory);
        this.lock = new ReentrantReadWriteLock();
    }

    @Override
    public void init() throws IOException {
        try {
            log.debug("initialization started");

            // Perform the initial refresh to ensure we have a well-constructed object.
            // This doesn't need to be guarded as long as we lock within the method itself.
            refresh();

            FileWatchCallback callback = new FileWatchCallback() {

                @Override
                public void fileCreated(Path path) throws IOException {
                    log.debug("File created event triggered for {}", path);
                    refresh();
                }

                @Override
                public void fileDeleted(Path path) throws IOException {
                    log.debug("File deleted event triggered for {}", path);
                    refresh();
                }

                @Override
                public void fileModified(Path path) throws IOException {
                    log.debug("File modified event triggered for {}", path);
                    refresh();
                }

            };

            try {
                lock.writeLock().lock();

                fileWatchService = new FileWatchService(this.pemDirectory, callback);
                fileWatchService.init();
            } finally {
                lock.writeLock().unlock();
            }
        } finally {
            log.debug("initialization completed");
        }
    }

    @Override
    public void close() throws IOException {
        try {
            log.debug("close started");

            lock.writeLock().lock();

            if (fileWatchService != null) {
                try {
                    fileWatchService.close();
                } catch (IOException e) {
                    log.warn(String.format("%s encountered error during close", pemDirectory), e);
                } finally {
                    fileWatchService = null;
                }
            }
        } finally {
            lock.writeLock().unlock();

            log.debug("close completed");
        }
    }

    @Override
    public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
        VerificationKeyResolver localDelegate;

        try {
            lock.readLock().lock();
            localDelegate = delegate;
        } finally {
            lock.readLock().unlock();
        }

        if (localDelegate == null)
            throw new UnresolvableKeyException("VerificationKeyResolver delegate is null");

        return localDelegate.resolveKey(jws, nestingContext);
    }

    private void refresh() throws IOException {
        // Since we're updating our delegate, make sure to surround with the write lock!
        try {
            log.info("VerificationKeyResolver delegate refresh started");
            lock.writeLock().lock();
            delegate = factory.create();
        } finally {
            lock.writeLock().unlock();
            log.info("VerificationKeyResolver delegate refresh completed");
        }
    }

}
