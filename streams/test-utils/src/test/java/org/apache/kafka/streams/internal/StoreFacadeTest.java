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
package org.apache.kafka.streams.internal;

import org.apache.kafka.streams.processor.StateStore;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(EasyMockRunner.class)
public class StoreFacadeTest {
    @SuppressWarnings("UnusedDeclaration")
    @Mock(type = MockType.NICE)
    private StateStore inner;

    private StoreFacade storeFacade;

    @Before
    public void before() {
        storeFacade = new StoreFacade(inner);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullStore() {
        new StoreFacade(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowToInitializeStore() {
        storeFacade.init(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowToCloseStore() {
        storeFacade.close();
    }

    @Test
    public void shouldGetNameFromInnerStore() {
        inner.name();
        expectLastCall().andReturn("storeName");
        replay(inner);

        assertThat(storeFacade.name(), equalTo("storeName"));
    }

    @Test
    public void shouldFlushInnerStore() {
        inner.flush();
        expectLastCall();
        replay(inner);

        storeFacade.flush();

        verify(inner);
    }

    @Test
    public void shouldGetPersistenceFromInner() {
        inner.persistent();
        expectLastCall().andReturn(false).andReturn(true);
        replay(inner);

        assertThat(storeFacade.persistent(), equalTo(false));
        assertThat(storeFacade.persistent(), equalTo(true));
    }

    @Test
    public void shouldGetIsOpenFromInner() {
        inner.isOpen();
        expectLastCall().andReturn(false).andReturn(true);
        replay(inner);

        assertThat(storeFacade.isOpen(), equalTo(false));
        assertThat(storeFacade.isOpen(), equalTo(true));
    }
}
