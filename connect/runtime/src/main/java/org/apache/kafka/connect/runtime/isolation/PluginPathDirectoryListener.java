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
package org.apache.kafka.connect.runtime.isolation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class PluginPathDirectoryListener implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PluginPathDirectoryListener.class);

    private final PluginReceiver pluginReceiver;
    private final WatchService watchService;
    private final Set<Path> pluginDirectories;

    PluginPathDirectoryListener(
            PluginReceiver pluginReceiver,
            Iterable<Path> pluginDirectories
    ) throws IOException {
        this.pluginReceiver = pluginReceiver;
        this.pluginDirectories = new HashSet<>();

        watchService = FileSystems.getDefault().newWatchService();
        for (Path pluginDirectory : pluginDirectories) {
            registerDirectory(pluginDirectory);
        }
    }

    private void registerDirectory(Path pluginPathDirectory) throws IOException {
        if (!pluginPathDirectory.toFile().isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + pluginPathDirectory);
        }
        pluginPathDirectory.register(
                watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE
        );

        // Have to track which entries in the plugin path directory are directories; after a
        // file/directory is deleted, it's impossible to determine whether or not it used to be a
        // directory.
        for (File file : pluginPathDirectory.toFile().listFiles()) {
            if (file.isDirectory()) {
                pluginDirectories.add(file.toPath().toAbsolutePath());
            }
        }
    }

    @Override
    public void run() {
        for (;;) {
            try {
                WatchKey watchKey = watchService.take();
                List<WatchEvent<?>> events = watchKey.pollEvents();
                Path pluginPath = watchKeyPath(watchKey);
                for (WatchEvent<?> event : events) {
                    handleEvent(pluginPath, pathWatchEvent(event));
                }
                if (!watchKey.reset()) {
                    log.warn("No longer monitoring directory {} on plugin path", pluginPath);
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted while polling for plugin path file system changes; polling will cease");
                return;
            } catch (Throwable t) {
                log.error("Error while monitoring file system for plugin path changes", t);
                t.printStackTrace(System.out);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private WatchEvent<Path> pathWatchEvent(WatchEvent<?> watchEvent) {
        return (WatchEvent<Path>) watchEvent;
    }

    @SuppressWarnings("unchecked")
    private Path watchKeyPath(WatchKey watchKey) {
        return (Path) watchKey.watchable();
    }

    private void handleEvent(Path pluginPath, WatchEvent<Path> event) {
        Path pluginLocation = pluginPath.resolve(event.context()).toAbsolutePath();
        if (StandardWatchEventKinds.ENTRY_CREATE.equals(event.kind())) {
            boolean isDirectory = Files.isDirectory(pluginLocation);
            if (isDirectory) {
                pluginDirectories.add(pluginLocation);
            }
            if (isDirectory || PluginUtils.isArchive(pluginLocation)) {
                pluginReceiver.onCreate(pluginLocation);
            }
        } else if (StandardWatchEventKinds.ENTRY_DELETE.equals(event.kind())) {
            boolean wasDirectory = pluginDirectories.remove(pluginLocation);
            if (wasDirectory || PluginUtils.isArchive(pluginLocation)) {
                pluginReceiver.onDelete(pluginLocation);
            }
        } else {
            log.warn(
                    "Received unexpected event kind while monitoring file system for changes: {}",
                    event.kind()
            );
        }
    }

    interface PluginReceiver {

        /**
          * Called when an entry that could be a plugin (either a directory or an archive file) is
          * created.
          * @param pluginLocation The absolute path to the newly-created directory/file
          */
        void onCreate(Path pluginLocation);

        /**
          * Called when an entry that could have been a plugin (either a directory or an archive file) is
          * deleted.
          * @param pluginLocation The absolute path to the recently-deleted directory/file
          */
        void onDelete(Path pluginLocation);
    }
}
