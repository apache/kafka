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

package org.apache.kafka.controller;

import org.apache.kafka.server.common.KRaftVersion;

/**
 * Type for upgrading and reading the kraft version.
 */
interface KRaftVersionAccessor {
    /**
     * Returns the latest kraft version.
     *
     * The latest version may be uncommitted.
     */
    KRaftVersion kraftVersion();

    /**
     * Upgrade the kraft version.
     *
     * @param epoch the current epoch
     * @param newVersion the new kraft version to upgrade to
     * @param validateOnly whether to just validate the change and not persist it
     */
    void upgradeKRaftVersion(int epoch, KRaftVersion newVersion, boolean validateOnly);
}
