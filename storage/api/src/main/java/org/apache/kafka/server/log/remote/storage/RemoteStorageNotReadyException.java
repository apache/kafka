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
 * This exception is thrown when a remote storage operation cannot proceed because the remote storage is not ready.
 * This may occur in situations where the remote storage (or) metadata layer is initializing, unreachable,
 * or temporarily unavailable.
 * <p>
 * Instances of this exception indicate that the error is retriable, and the operation might
 * succeed if attempted again when the remote storage (or) metadata layer becomes operational.
 */
public class RemoteStorageNotReadyException extends RetriableRemoteStorageException {

    public RemoteStorageNotReadyException(String message) {
        super(message);
    }

    public RemoteStorageNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }

    public RemoteStorageNotReadyException(Throwable cause) {
        super(cause);
    }
}
