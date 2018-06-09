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

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.function.Supplier;


/**
 * an MBeanInfo subclass that lazily calculates attributes
 */
public class LazyMBeanInfo extends MBeanInfo implements Serializable  {
    private final Supplier<MBeanAttributeInfo[]> supplier;
    private volatile MBeanAttributeInfo[] lazyAttrs = null;

    public LazyMBeanInfo(
        String className,
        String description,
        MBeanConstructorInfo[] constructors,
        MBeanOperationInfo[] operations,
        MBeanNotificationInfo[] notifications,
        Supplier<MBeanAttributeInfo[]> supplier
    ) throws IllegalArgumentException {
        super(className, description, null, constructors, operations, notifications);
        this.supplier = supplier;
    }

    @Override
    public MBeanAttributeInfo[] getAttributes() {
        MBeanAttributeInfo[] val = lazyAttrs;
        if (val != null) {
            return val.clone(); //match upstream behaviour
        }
        val = supplier.get();
        if (val == null) {
            val = new MBeanAttributeInfo[0];
        }
        lazyAttrs = val;
        return val.clone();
    }

    /**
     * JMX uses RMI, which relies on serializing MBeans over to the remote (jconsole) JVM.
     * This means we cant ship any custom classes over, as they would not be found for
     * de-serializations.
     * @return a vanilla {@link MBeanInfo} instance in our stead
     * @throws ObjectStreamException
     */
    protected Object writeReplace() throws ObjectStreamException {
        return new MBeanInfo(
                getClassName(), 
                getDescription(), 
                getAttributes(), //materializes the attributes 
                getConstructors(), 
                getOperations(),
                getNotifications(),
                getDescriptor()
        );
    }
}
