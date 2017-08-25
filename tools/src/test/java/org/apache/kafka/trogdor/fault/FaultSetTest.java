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

package org.apache.kafka.trogdor.fault;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class FaultSetTest {
    private static final NoOpFault FAULT_A =
        new NoOpFault("faultA", new NoOpFaultSpec(0, 100));

    private static final NoOpFault FAULT_B =
        new NoOpFault("faultB", new NoOpFaultSpec(20, 60));

    private static final NoOpFault FAULT_C =
        new NoOpFault("faultC", new NoOpFaultSpec(40, 50));

    private static final NoOpFault FAULT_D =
        new NoOpFault("faultD", new NoOpFaultSpec(50, 10));

    private static final List<Fault> FAULTS_IN_START_ORDER =
        Arrays.<Fault>asList(FAULT_A, FAULT_B, FAULT_C, FAULT_D);

    private static final List<Fault> FAULTS_IN_END_ORDER =
        Arrays.<Fault>asList(FAULT_D, FAULT_B, FAULT_C, FAULT_A);

    @Test
    public void testIterateByStart() throws Exception {
        FaultSet faultSet = new FaultSet();
        for (Fault fault: FAULTS_IN_END_ORDER) {
            faultSet.add(fault);
        }
        int i = 0;
        for (Iterator<Fault> iter = faultSet.iterateByStart(); iter.hasNext(); ) {
            Fault fault = iter.next();
            assertEquals(FAULTS_IN_START_ORDER.get(i), fault);
            i++;
        }
    }

    @Test
    public void testIterateByEnd() throws Exception {
        FaultSet faultSet = new FaultSet();
        for (Fault fault: FAULTS_IN_START_ORDER) {
            faultSet.add(fault);
        }
        int i = 0;
        for (Iterator<Fault> iter = faultSet.iterateByEnd(); iter.hasNext(); ) {
            Fault fault = iter.next();
            assertEquals(FAULTS_IN_END_ORDER.get(i), fault);
            i++;
        }
    }

    @Test
    public void testDeletes() throws Exception {
        FaultSet faultSet = new FaultSet();
        for (Fault fault: FAULTS_IN_START_ORDER) {
            faultSet.add(fault);
        }
        Iterator<Fault> iter = faultSet.iterateByEnd();
        iter.next();
        iter.next();
        iter.remove();
        iter.next();
        iter.next();
        iter.remove();
        assertFalse(iter.hasNext());
        try {
            iter.next();
            fail("expected NoSuchElementException");
        } catch (NoSuchElementException e) {
        }
        iter = faultSet.iterateByEnd();
        assertEquals(FAULT_D, iter.next());
        assertEquals(FAULT_C, iter.next());
        assertFalse(iter.hasNext());
        iter = faultSet.iterateByStart();
        faultSet.remove(FAULT_C);
        assertEquals(FAULT_D, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testEqualRanges() throws Exception {
        FaultSet faultSet = new FaultSet();
        faultSet.add(new NoOpFault("fault1", new NoOpFaultSpec(10, 20)));
        faultSet.add(new NoOpFault("fault2", new NoOpFaultSpec(10, 20)));
        faultSet.add(new NoOpFault("fault3", new NoOpFaultSpec(10, 20)));
        faultSet.add(new NoOpFault("fault4", new NoOpFaultSpec(10, 20)));
        for (Iterator<Fault> iter = faultSet.iterateByStart(); iter.hasNext(); ) {
            Fault fault = iter.next();
            if (fault.id().equals("fault3")) {
                iter.remove();
            }
        }
        Iterator<Fault> iter = faultSet.iterateByStart();
        assertEquals("fault1", iter.next().id());
        assertEquals("fault2", iter.next().id());
        assertEquals("fault4", iter.next().id());
        assertFalse(iter.hasNext());
    }
};
