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
package org.apache.kafka.common.security.oauthbearer.internals.expiring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredentialRefreshingLogin.LoginContextFactory;
import org.apache.kafka.common.utils.MockScheduler;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class ExpiringCredentialRefreshingLoginTest {
    private static final Configuration EMPTY_WILDCARD_CONFIGURATION;
    static {
        EMPTY_WILDCARD_CONFIGURATION = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[0]; // match any name
            }
        };
    }

    /*
     * An ExpiringCredentialRefreshingLogin that we can tell explicitly to
     * create/remove an expiring credential with specific
     * create/expire/absoluteLastRefresh times
     */
    private static class TestExpiringCredentialRefreshingLogin extends ExpiringCredentialRefreshingLogin {
        private ExpiringCredential expiringCredential;
        private ExpiringCredential tmpExpiringCredential;
        private final Time time;
        private final long lifetimeMillis;
        private final long absoluteLastRefreshTimeMs;
        private final boolean clientReloginAllowedBeforeLogout;

        public TestExpiringCredentialRefreshingLogin(ExpiringCredentialRefreshConfig refreshConfig,
                LoginContextFactory loginContextFactory, Time time, final long lifetimeMillis,
                final long absoluteLastRefreshMs, boolean clientReloginAllowedBeforeLogout) {
            super("contextName", EMPTY_WILDCARD_CONFIGURATION, refreshConfig, null,
                    TestExpiringCredentialRefreshingLogin.class, loginContextFactory, Objects.requireNonNull(time));
            this.time = time;
            this.lifetimeMillis = lifetimeMillis;
            this.absoluteLastRefreshTimeMs = absoluteLastRefreshMs;
            this.clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout;
        }

        public long getCreateMs() {
            return time.milliseconds();
        }

        public long getExpireTimeMs() {
            return time.milliseconds() + lifetimeMillis;
        }

        /*
         * Invoke at login time
         */
        public void createNewExpiringCredential() {
            if (!clientReloginAllowedBeforeLogout)
                /*
                 * Was preceded by logout
                 */
                expiringCredential = internalNewExpiringCredential();
            else {
                boolean initialLogin = expiringCredential == null;
                if (initialLogin)
                    // no logout immediately after the initial login
                    this.expiringCredential = internalNewExpiringCredential();
                else
                    /*
                     * This is at least the second invocation of login; we will move the credential
                     * over upon logout, which should be invoked next
                     */
                    this.tmpExpiringCredential = internalNewExpiringCredential();
            }
        }

        /*
         * Invoke at logout time
         */
        public void clearExpiringCredential() {
            if (!clientReloginAllowedBeforeLogout)
                /*
                 * Have not yet invoked login
                 */
                expiringCredential = null;
            else
                /*
                 * login has already been invoked
                 */
                expiringCredential = tmpExpiringCredential;
        }

        @Override
        public ExpiringCredential expiringCredential() {
            return expiringCredential;
        }

        private ExpiringCredential internalNewExpiringCredential() {
            return new ExpiringCredential() {
                private final long createMs = getCreateMs();
                private final long expireTimeMs = getExpireTimeMs();

                @Override
                public String principalName() {
                    return "Created at " + new Date(createMs);
                }

                @Override
                public Long startTimeMs() {
                    return createMs;
                }

                @Override
                public long expireTimeMs() {
                    return expireTimeMs;
                }

                @Override
                public Long absoluteLastRefreshTimeMs() {
                    return absoluteLastRefreshTimeMs;
                }

                // useful in debugger
                @Override
                public String toString() {
                    return String.format("startTimeMs=%d, expireTimeMs=%d, absoluteLastRefreshTimeMs=%s", startTimeMs(),
                            expireTimeMs(), absoluteLastRefreshTimeMs());
                }

            };
        }
    }

    /*
     * A class that will forward all login/logout/getSubject() calls to a mock while
     * also telling an instance of TestExpiringCredentialRefreshingLogin to
     * create/remove an expiring credential upon login/logout(). Basically we are
     * getting the functionality of a mock while simultaneously in the same method
     * call performing creation/removal of expiring credentials.
     */
    private static class TestLoginContext extends LoginContext {
        private final TestExpiringCredentialRefreshingLogin testExpiringCredentialRefreshingLogin;
        private final LoginContext mockLoginContext;

        public TestLoginContext(TestExpiringCredentialRefreshingLogin testExpiringCredentialRefreshingLogin,
                LoginContext mockLoginContext) throws LoginException {
            super("contextName", null, null, EMPTY_WILDCARD_CONFIGURATION);
            this.testExpiringCredentialRefreshingLogin = Objects.requireNonNull(testExpiringCredentialRefreshingLogin);
            // sanity check to make sure it is likely a mock
            if (Objects.requireNonNull(mockLoginContext).getClass().equals(LoginContext.class)
                    || mockLoginContext.getClass().equals(getClass()))
                throw new IllegalArgumentException();
            this.mockLoginContext = mockLoginContext;
        }

        @Override
        public void login() throws LoginException {
            /*
             * Here is where we get the functionality of a mock while simultaneously
             * performing the creation of an expiring credential
             */
            mockLoginContext.login();
            testExpiringCredentialRefreshingLogin.createNewExpiringCredential();
        }

        @Override
        public void logout() throws LoginException {
            /*
             * Here is where we get the functionality of a mock while simultaneously
             * performing the removal of an expiring credential
             */
            mockLoginContext.logout();
            testExpiringCredentialRefreshingLogin.clearExpiringCredential();
        }

        @Override
        public Subject getSubject() {
            // here we just need the functionality of a mock
            return mockLoginContext.getSubject();
        }
    }

    /*
     * An implementation of LoginContextFactory that returns an instance of
     * TestLoginContext
     */
    private static class TestLoginContextFactory extends LoginContextFactory {
        private final KafkaFutureImpl<Object> refresherThreadStartedFuture = new KafkaFutureImpl<>();
        private final KafkaFutureImpl<Object> refresherThreadDoneFuture = new KafkaFutureImpl<>();
        private TestLoginContext testLoginContext;

        public void configure(LoginContext mockLoginContext,
                TestExpiringCredentialRefreshingLogin testExpiringCredentialRefreshingLogin) throws LoginException {
            // sanity check to make sure it is likely a mock
            if (Objects.requireNonNull(mockLoginContext).getClass().equals(LoginContext.class)
                    || mockLoginContext.getClass().equals(TestLoginContext.class))
                throw new IllegalArgumentException();
            this.testLoginContext = new TestLoginContext(Objects.requireNonNull(testExpiringCredentialRefreshingLogin),
                    mockLoginContext);
        }

        @Override
        public LoginContext createLoginContext(ExpiringCredentialRefreshingLogin expiringCredentialRefreshingLogin) throws LoginException {
            return new LoginContext("", null, null, EMPTY_WILDCARD_CONFIGURATION) {
                private boolean loginSuccess = false;
                @Override
                public void login() throws LoginException {
                    testLoginContext.login();
                    loginSuccess = true;
                }
        
                @Override
                public void logout() throws LoginException {
                    if (!loginSuccess)
                        // will cause the refresher thread to exit
                        throw new IllegalStateException("logout called without a successful login");
                    testLoginContext.logout();
                }
        
                @Override
                public Subject getSubject() {
                    return testLoginContext.getSubject();
                }
            };
        }

        @Override
        public void refresherThreadStarted() {
            refresherThreadStartedFuture.complete(null);
        }

        @Override
        public void refresherThreadDone() {
            refresherThreadDoneFuture.complete(null);
        }

        public Future<?> refresherThreadStartedFuture() {
            return refresherThreadStartedFuture;
        }

        public Future<?> refresherThreadDoneFuture() {
            return refresherThreadDoneFuture;
        }
    }

    @Test
    public void testRefresh() throws Exception {
        for (int numExpectedRefreshes : new int[] {0, 1, 2}) {
            for (boolean clientReloginAllowedBeforeLogout : new boolean[] {true, false}) {
                Subject subject = new Subject();
                final LoginContext mockLoginContext = mock(LoginContext.class);
                when(mockLoginContext.getSubject()).thenReturn(subject);

                MockTime mockTime = new MockTime();
                long startMs = mockTime.milliseconds();
                /*
                 * Identify the lifetime of each expiring credential
                 */
                long lifetimeMinutes = 100L;
                /*
                 * Identify the point at which refresh will occur in that lifetime
                 */
                long refreshEveryMinutes = 80L;
                /*
                 * Set an absolute last refresh time that will cause the login thread to exit
                 * after a certain number of re-logins (by adding an extra half of a refresh
                 * interval).
                 */
                long absoluteLastRefreshMs = startMs + (1 + numExpectedRefreshes) * 1000 * 60 * refreshEveryMinutes
                        - 1000 * 60 * refreshEveryMinutes / 2;
                /*
                 * Identify buffer time on either side for the refresh algorithm
                 */
                short minPeriodSeconds = (short) 0;
                short bufferSeconds = minPeriodSeconds;

                /*
                 * Define some listeners so we can keep track of who gets done and when. All
                 * added listeners should end up done except the last, extra one, which should
                 * not.
                 */
                MockScheduler mockScheduler = new MockScheduler(mockTime);
                List<KafkaFutureImpl<Long>> waiters = addWaiters(mockScheduler, 1000 * 60 * refreshEveryMinutes,
                        numExpectedRefreshes + 1);

                // Create the ExpiringCredentialRefreshingLogin instance under test
                TestLoginContextFactory testLoginContextFactory = new TestLoginContextFactory();
                TestExpiringCredentialRefreshingLogin testExpiringCredentialRefreshingLogin = new TestExpiringCredentialRefreshingLogin(
                        refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
                                1.0 * refreshEveryMinutes / lifetimeMinutes, minPeriodSeconds, bufferSeconds,
                                clientReloginAllowedBeforeLogout),
                        testLoginContextFactory, mockTime, 1000 * 60 * lifetimeMinutes, absoluteLastRefreshMs,
                        clientReloginAllowedBeforeLogout);
                testLoginContextFactory.configure(mockLoginContext, testExpiringCredentialRefreshingLogin);

                /*
                 * Perform the login, wait up to a certain amount of time for the refresher
                 * thread to exit, and make sure the correct calls happened at the correct times
                 */
                long expectedFinalMs = startMs + numExpectedRefreshes * 1000 * 60 * refreshEveryMinutes;
                assertFalse(testLoginContextFactory.refresherThreadStartedFuture().isDone());
                assertFalse(testLoginContextFactory.refresherThreadDoneFuture().isDone());
                testExpiringCredentialRefreshingLogin.login();
                assertTrue(testLoginContextFactory.refresherThreadStartedFuture().isDone());
                testLoginContextFactory.refresherThreadDoneFuture().get(1L, TimeUnit.SECONDS);
                assertEquals(expectedFinalMs, mockTime.milliseconds());
                for (int i = 0; i < numExpectedRefreshes; ++i) {
                    KafkaFutureImpl<Long> waiter = waiters.get(i);
                    assertTrue(waiter.isDone());
                    assertEquals((i + 1) * 1000 * 60 * refreshEveryMinutes, waiter.get().longValue() - startMs);
                }
                assertFalse(waiters.get(numExpectedRefreshes).isDone());

                /*
                 * We expect login() to be invoked followed by getSubject() and then ultimately followed by
                 * numExpectedRefreshes pairs of either login()/logout() or logout()/login() calls
                 */
                InOrder inOrder = inOrder(mockLoginContext);
                inOrder.verify(mockLoginContext).login();
                inOrder.verify(mockLoginContext).getSubject();
                for (int i = 0; i < numExpectedRefreshes; ++i) {
                    if (clientReloginAllowedBeforeLogout) {
                        inOrder.verify(mockLoginContext).login();
                        inOrder.verify(mockLoginContext).logout();
                    } else {
                        inOrder.verify(mockLoginContext).logout();
                        inOrder.verify(mockLoginContext).login();
                    }
                }
            }
        }
    }

    @Test
    public void testRefreshWithExpirationSmallerThanConfiguredBuffers() throws Exception {
        int numExpectedRefreshes = 1;
        boolean clientReloginAllowedBeforeLogout = true;
        final LoginContext mockLoginContext = mock(LoginContext.class);
        Subject subject = new Subject();
        when(mockLoginContext.getSubject()).thenReturn(subject);

        MockTime mockTime = new MockTime();
        long startMs = mockTime.milliseconds();
        /*
         * Identify the lifetime of each expiring credential
         */
        long lifetimeMinutes = 10L;
        /*
         * Identify the point at which refresh will occur in that lifetime
         */
        long refreshEveryMinutes = 8L;
        /*
         * Set an absolute last refresh time that will cause the login thread to exit
         * after a certain number of re-logins (by adding an extra half of a refresh
         * interval).
         */
        long absoluteLastRefreshMs = startMs + (1 + numExpectedRefreshes) * 1000 * 60 * refreshEveryMinutes
                - 1000 * 60 * refreshEveryMinutes / 2;
        /*
         * Identify buffer time on either side for the refresh algorithm that will cause
         * the entire lifetime to be taken up. In other words, make sure there is no way
         * to honor the buffers.
         */
        short minPeriodSeconds = (short) (1 + lifetimeMinutes * 60 / 2);
        short bufferSeconds = minPeriodSeconds;

        /*
         * Define some listeners so we can keep track of who gets done and when. All
         * added listeners should end up done except the last, extra one, which should
         * not.
         */
        MockScheduler mockScheduler = new MockScheduler(mockTime);
        List<KafkaFutureImpl<Long>> waiters = addWaiters(mockScheduler, 1000 * 60 * refreshEveryMinutes,
                numExpectedRefreshes + 1);

        // Create the ExpiringCredentialRefreshingLogin instance under test
        TestLoginContextFactory testLoginContextFactory = new TestLoginContextFactory();
        TestExpiringCredentialRefreshingLogin testExpiringCredentialRefreshingLogin = new TestExpiringCredentialRefreshingLogin(
                refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
                        1.0 * refreshEveryMinutes / lifetimeMinutes, minPeriodSeconds, bufferSeconds,
                        clientReloginAllowedBeforeLogout),
                testLoginContextFactory, mockTime, 1000 * 60 * lifetimeMinutes, absoluteLastRefreshMs,
                clientReloginAllowedBeforeLogout);
        testLoginContextFactory.configure(mockLoginContext, testExpiringCredentialRefreshingLogin);

        /*
         * Perform the login, wait up to a certain amount of time for the refresher
         * thread to exit, and make sure the correct calls happened at the correct times
         */
        long expectedFinalMs = startMs + numExpectedRefreshes * 1000 * 60 * refreshEveryMinutes;
        assertFalse(testLoginContextFactory.refresherThreadStartedFuture().isDone());
        assertFalse(testLoginContextFactory.refresherThreadDoneFuture().isDone());
        testExpiringCredentialRefreshingLogin.login();
        assertTrue(testLoginContextFactory.refresherThreadStartedFuture().isDone());
        testLoginContextFactory.refresherThreadDoneFuture().get(1L, TimeUnit.SECONDS);
        assertEquals(expectedFinalMs, mockTime.milliseconds());
        for (int i = 0; i < numExpectedRefreshes; ++i) {
            KafkaFutureImpl<Long> waiter = waiters.get(i);
            assertTrue(waiter.isDone());
            assertEquals((i + 1) * 1000 * 60 * refreshEveryMinutes, waiter.get().longValue() - startMs);
        }
        assertFalse(waiters.get(numExpectedRefreshes).isDone());

        InOrder inOrder = inOrder(mockLoginContext);
        inOrder.verify(mockLoginContext).login();
        for (int i = 0; i < numExpectedRefreshes; ++i) {
            inOrder.verify(mockLoginContext).login();
            inOrder.verify(mockLoginContext).logout();
        }
    }

    @Test
    public void testRefreshWithExpirationSmallerThanConfiguredBuffersAndOlderCreateTime() throws Exception {
        int numExpectedRefreshes = 1;
        boolean clientReloginAllowedBeforeLogout = true;
        final LoginContext mockLoginContext = mock(LoginContext.class);
        Subject subject = new Subject();
        when(mockLoginContext.getSubject()).thenReturn(subject);

        MockTime mockTime = new MockTime();
        long startMs = mockTime.milliseconds();
        /*
         * Identify the lifetime of each expiring credential
         */
        long lifetimeMinutes = 10L;
        /*
         * Identify the point at which refresh will occur in that lifetime
         */
        long refreshEveryMinutes = 8L;
        /*
         * Set an absolute last refresh time that will cause the login thread to exit
         * after a certain number of re-logins (by adding an extra half of a refresh
         * interval).
         */
        long absoluteLastRefreshMs = startMs + (1 + numExpectedRefreshes) * 1000 * 60 * refreshEveryMinutes
                - 1000 * 60 * refreshEveryMinutes / 2;
        /*
         * Identify buffer time on either side for the refresh algorithm that will cause
         * the entire lifetime to be taken up. In other words, make sure there is no way
         * to honor the buffers.
         */
        short minPeriodSeconds = (short) (1 + lifetimeMinutes * 60 / 2);
        short bufferSeconds = minPeriodSeconds;

        /*
         * Define some listeners so we can keep track of who gets done and when. All
         * added listeners should end up done except the last, extra one, which should
         * not.
         */
        MockScheduler mockScheduler = new MockScheduler(mockTime);
        List<KafkaFutureImpl<Long>> waiters = addWaiters(mockScheduler, 1000 * 60 * refreshEveryMinutes,
                numExpectedRefreshes + 1);

        // Create the ExpiringCredentialRefreshingLogin instance under test
        TestLoginContextFactory testLoginContextFactory = new TestLoginContextFactory();
        TestExpiringCredentialRefreshingLogin testExpiringCredentialRefreshingLogin = new TestExpiringCredentialRefreshingLogin(
                refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
                        1.0 * refreshEveryMinutes / lifetimeMinutes, minPeriodSeconds, bufferSeconds,
                        clientReloginAllowedBeforeLogout),
                testLoginContextFactory, mockTime, 1000 * 60 * lifetimeMinutes, absoluteLastRefreshMs,
                clientReloginAllowedBeforeLogout) {

            @Override
            public long getCreateMs() {
                return super.getCreateMs() - 1000 * 60 * 60; // distant past
            }
        };
        testLoginContextFactory.configure(mockLoginContext, testExpiringCredentialRefreshingLogin);

        /*
         * Perform the login, wait up to a certain amount of time for the refresher
         * thread to exit, and make sure the correct calls happened at the correct times
         */
        long expectedFinalMs = startMs + numExpectedRefreshes * 1000 * 60 * refreshEveryMinutes;
        assertFalse(testLoginContextFactory.refresherThreadStartedFuture().isDone());
        assertFalse(testLoginContextFactory.refresherThreadDoneFuture().isDone());
        testExpiringCredentialRefreshingLogin.login();
        assertTrue(testLoginContextFactory.refresherThreadStartedFuture().isDone());
        testLoginContextFactory.refresherThreadDoneFuture().get(1L, TimeUnit.SECONDS);
        assertEquals(expectedFinalMs, mockTime.milliseconds());
        for (int i = 0; i < numExpectedRefreshes; ++i) {
            KafkaFutureImpl<Long> waiter = waiters.get(i);
            assertTrue(waiter.isDone());
            assertEquals((i + 1) * 1000 * 60 * refreshEveryMinutes, waiter.get().longValue() - startMs);
        }
        assertFalse(waiters.get(numExpectedRefreshes).isDone());

        InOrder inOrder = inOrder(mockLoginContext);
        inOrder.verify(mockLoginContext).login();
        for (int i = 0; i < numExpectedRefreshes; ++i) {
            inOrder.verify(mockLoginContext).login();
            inOrder.verify(mockLoginContext).logout();
        }
    }

    @Test
    public void testRefreshWithMinPeriodIntrusion() throws Exception {
        int numExpectedRefreshes = 1;
        boolean clientReloginAllowedBeforeLogout = true;
        Subject subject = new Subject();
        final LoginContext mockLoginContext = mock(LoginContext.class);
        when(mockLoginContext.getSubject()).thenReturn(subject);

        MockTime mockTime = new MockTime();
        long startMs = mockTime.milliseconds();
        /*
         * Identify the lifetime of each expiring credential
         */
        long lifetimeMinutes = 10L;
        /*
         * Identify the point at which refresh will occur in that lifetime
         */
        long refreshEveryMinutes = 8L;
        /*
         * Set an absolute last refresh time that will cause the login thread to exit
         * after a certain number of re-logins (by adding an extra half of a refresh
         * interval).
         */
        long absoluteLastRefreshMs = startMs + (1 + numExpectedRefreshes) * 1000 * 60 * refreshEveryMinutes
                - 1000 * 60 * refreshEveryMinutes / 2;

        /*
         * Identify a minimum period that will cause the refresh time to be delayed a
         * bit.
         */
        int bufferIntrusionSeconds = 1;
        short minPeriodSeconds = (short) (refreshEveryMinutes * 60 + bufferIntrusionSeconds);
        short bufferSeconds = (short) 0;

        /*
         * Define some listeners so we can keep track of who gets done and when. All
         * added listeners should end up done except the last, extra one, which should
         * not.
         */
        MockScheduler mockScheduler = new MockScheduler(mockTime);
        List<KafkaFutureImpl<Long>> waiters = addWaiters(mockScheduler,
                1000 * (60 * refreshEveryMinutes + bufferIntrusionSeconds), numExpectedRefreshes + 1);

        // Create the ExpiringCredentialRefreshingLogin instance under test
        TestLoginContextFactory testLoginContextFactory = new TestLoginContextFactory();
        TestExpiringCredentialRefreshingLogin testExpiringCredentialRefreshingLogin = new TestExpiringCredentialRefreshingLogin(
                refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
                        1.0 * refreshEveryMinutes / lifetimeMinutes, minPeriodSeconds, bufferSeconds,
                        clientReloginAllowedBeforeLogout),
                testLoginContextFactory, mockTime, 1000 * 60 * lifetimeMinutes, absoluteLastRefreshMs,
                clientReloginAllowedBeforeLogout);
        testLoginContextFactory.configure(mockLoginContext, testExpiringCredentialRefreshingLogin);

        /*
         * Perform the login, wait up to a certain amount of time for the refresher
         * thread to exit, and make sure the correct calls happened at the correct times
         */
        long expectedFinalMs = startMs
                + numExpectedRefreshes * 1000 * (60 * refreshEveryMinutes + bufferIntrusionSeconds);
        assertFalse(testLoginContextFactory.refresherThreadStartedFuture().isDone());
        assertFalse(testLoginContextFactory.refresherThreadDoneFuture().isDone());
        testExpiringCredentialRefreshingLogin.login();
        assertTrue(testLoginContextFactory.refresherThreadStartedFuture().isDone());
        testLoginContextFactory.refresherThreadDoneFuture().get(1L, TimeUnit.SECONDS);
        assertEquals(expectedFinalMs, mockTime.milliseconds());
        for (int i = 0; i < numExpectedRefreshes; ++i) {
            KafkaFutureImpl<Long> waiter = waiters.get(i);
            assertTrue(waiter.isDone());
            assertEquals((i + 1) * 1000 * (60 * refreshEveryMinutes + bufferIntrusionSeconds),
                    waiter.get().longValue() - startMs);
        }
        assertFalse(waiters.get(numExpectedRefreshes).isDone());

        InOrder inOrder = inOrder(mockLoginContext);
        inOrder.verify(mockLoginContext).login();
        for (int i = 0; i < numExpectedRefreshes; ++i) {
            inOrder.verify(mockLoginContext).login();
            inOrder.verify(mockLoginContext).logout();
        }
    }

    @Test
    public void testRefreshWithPreExpirationBufferIntrusion() throws Exception {
        int numExpectedRefreshes = 1;
        boolean clientReloginAllowedBeforeLogout = true;
        Subject subject = new Subject();
        final LoginContext mockLoginContext = mock(LoginContext.class);
        when(mockLoginContext.getSubject()).thenReturn(subject);

        MockTime mockTime = new MockTime();
        long startMs = mockTime.milliseconds();
        /*
         * Identify the lifetime of each expiring credential
         */
        long lifetimeMinutes = 10L;
        /*
         * Identify the point at which refresh will occur in that lifetime
         */
        long refreshEveryMinutes = 8L;
        /*
         * Set an absolute last refresh time that will cause the login thread to exit
         * after a certain number of re-logins (by adding an extra half of a refresh
         * interval).
         */
        long absoluteLastRefreshMs = startMs + (1 + numExpectedRefreshes) * 1000 * 60 * refreshEveryMinutes
                - 1000 * 60 * refreshEveryMinutes / 2;
        /*
         * Identify a minimum period that will cause the refresh time to be delayed a
         * bit.
         */
        int bufferIntrusionSeconds = 1;
        short bufferSeconds = (short) ((lifetimeMinutes - refreshEveryMinutes) * 60 + bufferIntrusionSeconds);
        short minPeriodSeconds = (short) 0;

        /*
         * Define some listeners so we can keep track of who gets done and when. All
         * added listeners should end up done except the last, extra one, which should
         * not.
         */
        MockScheduler mockScheduler = new MockScheduler(mockTime);
        List<KafkaFutureImpl<Long>> waiters = addWaiters(mockScheduler,
                1000 * (60 * refreshEveryMinutes - bufferIntrusionSeconds), numExpectedRefreshes + 1);

        // Create the ExpiringCredentialRefreshingLogin instance under test
        TestLoginContextFactory testLoginContextFactory = new TestLoginContextFactory();
        TestExpiringCredentialRefreshingLogin testExpiringCredentialRefreshingLogin = new TestExpiringCredentialRefreshingLogin(
                refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
                        1.0 * refreshEveryMinutes / lifetimeMinutes, minPeriodSeconds, bufferSeconds,
                        clientReloginAllowedBeforeLogout),
                testLoginContextFactory, mockTime, 1000 * 60 * lifetimeMinutes, absoluteLastRefreshMs,
                clientReloginAllowedBeforeLogout);
        testLoginContextFactory.configure(mockLoginContext, testExpiringCredentialRefreshingLogin);

        /*
         * Perform the login, wait up to a certain amount of time for the refresher
         * thread to exit, and make sure the correct calls happened at the correct times
         */
        long expectedFinalMs = startMs
                + numExpectedRefreshes * 1000 * (60 * refreshEveryMinutes - bufferIntrusionSeconds);
        assertFalse(testLoginContextFactory.refresherThreadStartedFuture().isDone());
        assertFalse(testLoginContextFactory.refresherThreadDoneFuture().isDone());
        testExpiringCredentialRefreshingLogin.login();
        assertTrue(testLoginContextFactory.refresherThreadStartedFuture().isDone());
        testLoginContextFactory.refresherThreadDoneFuture().get(1L, TimeUnit.SECONDS);
        assertEquals(expectedFinalMs, mockTime.milliseconds());
        for (int i = 0; i < numExpectedRefreshes; ++i) {
            KafkaFutureImpl<Long> waiter = waiters.get(i);
            assertTrue(waiter.isDone());
            assertEquals((i + 1) * 1000 * (60 * refreshEveryMinutes - bufferIntrusionSeconds),
                    waiter.get().longValue() - startMs);
        }
        assertFalse(waiters.get(numExpectedRefreshes).isDone());

        InOrder inOrder = inOrder(mockLoginContext);
        inOrder.verify(mockLoginContext).login();
        for (int i = 0; i < numExpectedRefreshes; ++i) {
            inOrder.verify(mockLoginContext).login();
            inOrder.verify(mockLoginContext).logout();
        }
    }

    @Test
    public void testLoginExceptionCausesCorrectLogout() throws Exception {
        int numExpectedRefreshes = 3;
        boolean clientReloginAllowedBeforeLogout = true;
        Subject subject = new Subject();
        final LoginContext mockLoginContext = mock(LoginContext.class);
        when(mockLoginContext.getSubject()).thenReturn(subject);
        Mockito.doNothing().doThrow(new LoginException()).doNothing().when(mockLoginContext).login();

        MockTime mockTime = new MockTime();
        long startMs = mockTime.milliseconds();
        /*
         * Identify the lifetime of each expiring credential
         */
        long lifetimeMinutes = 100L;
        /*
         * Identify the point at which refresh will occur in that lifetime
         */
        long refreshEveryMinutes = 80L;
        /*
         * Set an absolute last refresh time that will cause the login thread to exit
         * after a certain number of re-logins (by adding an extra half of a refresh
         * interval).
         */
        long absoluteLastRefreshMs = startMs + (1 + numExpectedRefreshes) * 1000 * 60 * refreshEveryMinutes
                - 1000 * 60 * refreshEveryMinutes / 2;
        /*
         * Identify buffer time on either side for the refresh algorithm
         */
        short minPeriodSeconds = (short) 0;
        short bufferSeconds = minPeriodSeconds;

        // Create the ExpiringCredentialRefreshingLogin instance under test
        TestLoginContextFactory testLoginContextFactory = new TestLoginContextFactory();
        TestExpiringCredentialRefreshingLogin testExpiringCredentialRefreshingLogin = new TestExpiringCredentialRefreshingLogin(
                refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
                        1.0 * refreshEveryMinutes / lifetimeMinutes, minPeriodSeconds, bufferSeconds,
                        clientReloginAllowedBeforeLogout),
                testLoginContextFactory, mockTime, 1000 * 60 * lifetimeMinutes, absoluteLastRefreshMs,
                clientReloginAllowedBeforeLogout);
        testLoginContextFactory.configure(mockLoginContext, testExpiringCredentialRefreshingLogin);

        /*
         * Perform the login and wait up to a certain amount of time for the refresher
         * thread to exit.  A timeout indicates the thread died due to logout()
         * being invoked on an instance where the login() invocation had failed.
         */
        assertFalse(testLoginContextFactory.refresherThreadStartedFuture().isDone());
        assertFalse(testLoginContextFactory.refresherThreadDoneFuture().isDone());
        testExpiringCredentialRefreshingLogin.login();
        assertTrue(testLoginContextFactory.refresherThreadStartedFuture().isDone());
        testLoginContextFactory.refresherThreadDoneFuture().get(1L, TimeUnit.SECONDS);
    }

    private static List<KafkaFutureImpl<Long>> addWaiters(MockScheduler mockScheduler, long refreshEveryMillis,
            int numWaiters) {
        List<KafkaFutureImpl<Long>> retvalWaiters = new ArrayList<>(numWaiters);
        for (int i = 1; i <= numWaiters; ++i) {
            KafkaFutureImpl<Long> waiter = new KafkaFutureImpl<Long>();
            mockScheduler.addWaiter(i * refreshEveryMillis, waiter);
            retvalWaiters.add(waiter);
        }
        return retvalWaiters;
    }

    private static ExpiringCredentialRefreshConfig refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
            double refreshWindowFactor, short minPeriodSeconds, short bufferSeconds,
            boolean clientReloginAllowedBeforeLogout) {
        Map<Object, Object> configs = new HashMap<>();
        configs.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, refreshWindowFactor);
        configs.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, 0);
        configs.put(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, minPeriodSeconds);
        configs.put(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, bufferSeconds);
        return new ExpiringCredentialRefreshConfig(new ConfigDef().withClientSaslSupport().parse(configs),
                clientReloginAllowedBeforeLogout);
    }
}
