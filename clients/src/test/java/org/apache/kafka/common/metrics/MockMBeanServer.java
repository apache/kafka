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

package org.apache.kafka.common.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.loading.ClassLoaderRepository;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class MockMBeanServer implements MBeanServer {
    private static final Logger log = LoggerFactory.getLogger(MockMBeanServer.class);

    final Map<ObjectName, Object> registered = new ConcurrentHashMap<>();
    CountDownLatch unregisterMbeanLatch = null;

    @Override
    public ObjectInstance createMBean(String className, ObjectName name) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object[] params, String[] signature) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectInstance registerMBean(Object object, ObjectName name) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
        log.info("registerMBean(object={}, name={})", object, name);
        registered.putIfAbsent(name, object);
        return null;
    }

    @Override
    public void unregisterMBean(ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException {
        if (unregisterMbeanLatch != null) {
            try {
                unregisterMbeanLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("unregisterMBean(name={})", name);
        registered.remove(name);
    }

    @Override
    public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRegistered(ObjectName name) {
        boolean result = registered.containsKey(name);
        log.info("isRegistered(name={}) = {}", name, result);
        return result;
    }

    @Override
    public Integer getMBeanCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getAttribute(ObjectName name, String attribute) throws MBeanException, AttributeNotFoundException, InstanceNotFoundException, ReflectionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public AttributeList getAttributes(ObjectName name, String[] attributes) throws InstanceNotFoundException, ReflectionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(ObjectName name, Attribute attribute) throws InstanceNotFoundException, AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new UnsupportedOperationException();

    }

    @Override
    public AttributeList setAttributes(ObjectName name, AttributeList attributes) throws InstanceNotFoundException, ReflectionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature) throws InstanceNotFoundException, MBeanException, ReflectionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDefaultDomain() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getDomains() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback) throws InstanceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback) throws InstanceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeNotificationListener(ObjectName name, ObjectName listener) throws InstanceNotFoundException, ListenerNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeNotificationListener(ObjectName name, NotificationListener listener) throws InstanceNotFoundException, ListenerNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public MBeanInfo getMBeanInfo(ObjectName name) throws InstanceNotFoundException, IntrospectionException, ReflectionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isInstanceOf(ObjectName name, String className) throws InstanceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object instantiate(String className) throws ReflectionException, MBeanException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object instantiate(String className, ObjectName loaderName) throws ReflectionException, MBeanException, InstanceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object instantiate(String className, Object[] params, String[] signature) throws ReflectionException, MBeanException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object instantiate(String className, ObjectName loaderName, Object[] params, String[] signature) throws ReflectionException, MBeanException, InstanceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override
    public ObjectInputStream deserialize(ObjectName name, byte[] data) throws InstanceNotFoundException, OperationsException {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override
    public ObjectInputStream deserialize(String className, byte[] data) throws OperationsException, ReflectionException {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override
    public ObjectInputStream deserialize(String className, ObjectName loaderName, byte[] data) throws InstanceNotFoundException, OperationsException, ReflectionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClassLoader getClassLoaderFor(ObjectName mbeanName) throws InstanceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClassLoader getClassLoader(ObjectName loaderName) throws InstanceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClassLoaderRepository getClassLoaderRepository() {
        throw new UnsupportedOperationException();
    }
}
