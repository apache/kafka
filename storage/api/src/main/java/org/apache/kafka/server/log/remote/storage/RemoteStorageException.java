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
package org.apache.kafka.server.log.remote.storage;

/**
 * Exception thrown when there is a remote storage error. This can be used as the base exception by implementors of
 * {@link RemoteStorageManager} or {@link RemoteLogMetadataManager} to create extended exceptions.
 */
public class RemoteStorageException extends Exception {
    private static final long serialVersionUID = 1L;

    public RemoteStorageException(final String message) {
        super(message);
    }

    public RemoteStorageException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public RemoteStorageException(Throwable cause) {
        super(cause);
    }
}
