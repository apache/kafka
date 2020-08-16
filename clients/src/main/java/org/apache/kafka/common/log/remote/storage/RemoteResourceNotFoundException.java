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
package org.apache.kafka.common.log.remote.storage;

import static java.lang.String.format;

/**
 * Exception thrown when a resource can not be found on the remote storage.
 *
 * A resource can be a log segment, any of the indexes or any which was stored in remote storage for a particular log
 * segment.
 */
public class RemoteResourceNotFoundException extends RemoteStorageException {

    public RemoteResourceNotFoundException(final RemoteLogSegmentId id, final String resourceName) {
        super(format("Requested resource associated to the remote log segment was not found. " +
                "ID: %s Resource name: %s", id.id(), resourceName));
    }

    public RemoteResourceNotFoundException(final String message) {
        super(message);
    }

    public RemoteResourceNotFoundException(final Throwable cause) {
        super("Requested remote resource was not found", cause);
    }
}
