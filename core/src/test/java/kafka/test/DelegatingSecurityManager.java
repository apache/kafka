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

package kafka.test;

import java.io.FileDescriptor;
import java.net.InetAddress;
import java.security.Permission;

/**
 * A {@link SecurityManager} implementation that accepts and wraps another {@link SecurityManager} instance.
 * This class is not intended to be used directly, but as a superclass. Subclasses can selectively modify parts of the
 * {@link SecurityManager} API without having to also implement boilerplate passthrough logic for the parts that
 * they do not intend to modify.
 */
@SuppressWarnings({"deprecation", "removal"})
public class DelegatingSecurityManager extends SecurityManager {
    private final SecurityManager originalSecurityManager;

    /**
     * Create a new {@link DelegatingSecurityManager}, which delegates all calls to {@link SecurityManager} API methods
     * to the provided {@link SecurityManager} instance.
     * @param originalSecurityManager the {@link SecurityManager} to delegate to; may be null
     */
    public DelegatingSecurityManager(SecurityManager originalSecurityManager) {
        this.originalSecurityManager = originalSecurityManager;
    }

    @Override
    public void checkExit(int status) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkExit(status);
    }

    @Override
    public Object getSecurityContext() {
        return (originalSecurityManager == null) ? super.getSecurityContext()
                : originalSecurityManager.getSecurityContext();
    }

    @Override
    public void checkPermission(Permission perm) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkPermission(perm);
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkPermission(perm, context);
    }

    @Override
    public void checkCreateClassLoader() {
        if (originalSecurityManager != null)
            originalSecurityManager.checkCreateClassLoader();
    }

    @Override
    public void checkAccess(Thread t) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkAccess(t);
    }

    @Override
    public void checkAccess(ThreadGroup g) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkAccess(g);
    }

    @Override
    public void checkExec(String cmd) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkExec(cmd);
    }

    @Override
    public void checkLink(String lib) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkLink(lib);
    }

    @Override
    public void checkRead(FileDescriptor fd) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkRead(fd);
    }

    @Override
    public void checkRead(String file) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkRead(file);
    }

    @Override
    public void checkRead(String file, Object context) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkRead(file, context);
    }

    @Override
    public void checkWrite(FileDescriptor fd) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkWrite(fd);
    }

    @Override
    public void checkWrite(String file) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkWrite(file);
    }

    @Override
    public void checkDelete(String file) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkDelete(file);
    }

    @Override
    public void checkConnect(String host, int port) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkConnect(host, port);
    }

    @Override
    public void checkConnect(String host, int port, Object context) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkConnect(host, port, context);
    }

    @Override
    public void checkListen(int port) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkListen(port);
    }

    @Override
    public void checkAccept(String host, int port) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkAccept(host, port);
    }

    @Override
    public void checkMulticast(InetAddress maddr) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkMulticast(maddr);
    }

    @Override
    public void checkMulticast(InetAddress maddr, byte ttl) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkMulticast(maddr, ttl);
    }

    @Override
    public void checkPropertiesAccess() {
        if (originalSecurityManager != null)
            originalSecurityManager.checkPropertiesAccess();
    }

    @Override
    public void checkPropertyAccess(String key) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkPropertyAccess(key);
    }

    @Override
    public void checkPrintJobAccess() {
        if (originalSecurityManager != null)
            originalSecurityManager.checkPrintJobAccess();
    }

    @Override
    public void checkPackageAccess(String pkg) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkPackageAccess(pkg);
    }

    @Override
    public void checkPackageDefinition(String pkg) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkPackageDefinition(pkg);
    }

    @Override
    public void checkSetFactory() {
        if (originalSecurityManager != null)
            originalSecurityManager.checkSetFactory();
    }

    @Override
    public void checkSecurityAccess(String target) {
        if (originalSecurityManager != null)
            originalSecurityManager.checkSecurityAccess(target);
    }

    @Override
    public ThreadGroup getThreadGroup() {
        return (originalSecurityManager == null) ? super.getThreadGroup()
                : originalSecurityManager.getThreadGroup();
    }
}
