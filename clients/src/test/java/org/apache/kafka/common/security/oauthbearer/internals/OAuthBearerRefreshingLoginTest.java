package org.apache.kafka.common.security.oauthbearer.internals;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredential;
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredentialRefreshingLogin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OAuthBearerRefreshingLoginTest {

    @InjectMocks
    private OAuthBearerRefreshingLogin oauthBearerRefreshingLogin;

    @Mock
    private Configuration mockConfiguration;
    @Mock
    private AuthenticateCallbackHandler mockLoginCallbackHandler;
    @Mock
    private Subject mockSubject;
    @Mock
    private OAuthBearerToken mockOAuthBearerToken;

    private Map<String, Object> mockConfigs;
    private String contextName;

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    @BeforeEach
    void setUp() {
        mockConfigs = new HashMap<>();
        // Use the correct types for the config values to avoid ClassCastException
        mockConfigs.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, 0.8);
        mockConfigs.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, 0.05);
        mockConfigs.put(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, (short) 60);
        mockConfigs.put(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, (short) 300);

        contextName = "testContext";
    }

    /**
     * Tests the configure method's side effects (initialization of the internal login instance)
     * and the logic of the overridden expiringCredential method, including cases with and without tokens.
     */
    @Test
    void testConfigureAndExpiringCredential() throws Exception {
        // 1. Call configure to initialize the private field expiringCredentialRefreshingLogin
        oauthBearerRefreshingLogin.configure(mockConfigs, contextName, mockConfiguration, mockLoginCallbackHandler);

        // 2. Retrieve the actual instance created by configure using reflection
        Field field = OAuthBearerRefreshingLogin.class.getDeclaredField("expiringCredentialRefreshingLogin");
        field.setAccessible(true);
        ExpiringCredentialRefreshingLogin actualInstance = (ExpiringCredentialRefreshingLogin) field.get(oauthBearerRefreshingLogin);
        assertNotNull(actualInstance, "expiringCredentialRefreshingLogin should be initialized after configure");

        // 3. Create a spy of the actual instance to mock its methods for testing the override
        ExpiringCredentialRefreshingLogin spyInstance = spy(actualInstance);
        // Replace the instance in the SUT with the spy
        setField(oauthBearerRefreshingLogin, "expiringCredentialRefreshingLogin", spyInstance);

        // --- Test case 1: No tokens in subject ---
        // Mock the subject() call on the spy to return an empty Subject
        when(spyInstance.subject()).thenReturn(new Subject());
        // Call the overridden method on the spy instance, NOT on oauthBearerRefreshingLogin
        // This call correctly invokes the overridden expiringCredential() method on the spy.
        ExpiringCredential credential = spyInstance.expiringCredential();
        assertNull(credential, "Credential should be null when subject has no tokens");
        verify(spyInstance).subject(); // Verify that subject() was called on the spy

        // --- Test case 2: Tokens in subject ---
        Subject subjectWithToken = new Subject();
        subjectWithToken.getPrivateCredentials().add(mockOAuthBearerToken);
        when(spyInstance.subject()).thenReturn(subjectWithToken);

        // Mock details of the token
        String principalName = "test-principal";
        Long startTimeMs = System.currentTimeMillis() - 3600000; // 1 hour ago
        Long lifetimeMs = 7200000L; // 2 hours lifetime
        when(mockOAuthBearerToken.principalName()).thenReturn(principalName);
        when(mockOAuthBearerToken.startTimeMs()).thenReturn(startTimeMs);
        when(mockOAuthBearerToken.lifetimeMs()).thenReturn(lifetimeMs);

        // Call the overridden method on the spy instance again to test token extraction
        // This call correctly invokes the overridden expiringCredential() method on the spy.
        ExpiringCredential tokenCredential = spyInstance.expiringCredential();

        assertNotNull(tokenCredential, "Credential should not be null when subject has tokens");
        assertEquals(principalName, tokenCredential.principalName(), "Principal name mismatch");
        assertEquals(startTimeMs, tokenCredential.startTimeMs(), "Start time mismatch");
        assertEquals(lifetimeMs, tokenCredential.expireTimeMs(), "Expire time mismatch");
        assertNull(tokenCredential.absoluteLastRefreshTimeMs(), "Absolute last refresh time should be null");

        verify(spyInstance, times(2)).subject(); // subject() was called twice in this test
    }

    @Test
    void testCloseWhenConfigured() throws Exception {
        // Call configure to ensure the private field expiringCredentialRefreshingLogin is initialized
        oauthBearerRefreshingLogin.configure(mockConfigs, contextName, mockConfiguration, mockLoginCallbackHandler);

        // Get the actual instance created by configure and spy on it
        ExpiringCredentialRefreshingLogin actualInstance = (ExpiringCredentialRefreshingLogin) getField(oauthBearerRefreshingLogin, "expiringCredentialRefreshingLogin");
        ExpiringCredentialRefreshingLogin spyInstance = spy(actualInstance);
        setField(oauthBearerRefreshingLogin, "expiringCredentialRefreshingLogin", spyInstance);

        // Call the close method
        oauthBearerRefreshingLogin.close();

        // Verify that close() was called on the underlying spy instance
        verify(spyInstance).close();
    }

    @Test
    void testCloseWhenNotConfigured() {
        // When close is called before configure, expiringCredentialRefreshingLogin is null.
        // The method should not throw an exception.
        oauthBearerRefreshingLogin.close();
        // No specific verification needed here, just ensuring it runs without error.
    }

    @Test
    void testSubjectWhenConfigured() throws Exception {
        // Configure the login object and spy on its internal instance
        oauthBearerRefreshingLogin.configure(mockConfigs, contextName, mockConfiguration, mockLoginCallbackHandler);
        ExpiringCredentialRefreshingLogin actualInstance = (ExpiringCredentialRefreshingLogin) getField(oauthBearerRefreshingLogin, "expiringCredentialRefreshingLogin");
        ExpiringCredentialRefreshingLogin spyInstance = spy(actualInstance);
        setField(oauthBearerRefreshingLogin, "expiringCredentialRefreshingLogin", spyInstance);

        // Mock the subject() call on the spy to return a mock Subject
        when(spyInstance.subject()).thenReturn(mockSubject);

        // Retrieve the subject and assert it matches the mocked one
        Subject retrievedSubject = oauthBearerRefreshingLogin.subject();

        assertEquals(mockSubject, retrievedSubject, "Subject should be delegated correctly from the internal login instance");
        verify(spyInstance).subject(); // Ensure subject() was called on the spy
    }

    @Test
    void testSubjectWhenNotConfigured() {
        // When subject() is called before configure, the internal login instance is null.
        // The method should return null.
        Subject retrievedSubject = oauthBearerRefreshingLogin.subject();
        assertNull(retrievedSubject, "Subject should be null when the login is not configured");
    }

    @Test
    void testServiceNameWhenConfigured() throws Exception {
        // Configure the login object and spy on its internal instance
        oauthBearerRefreshingLogin.configure(mockConfigs, contextName, mockConfiguration, mockLoginCallbackHandler);
        ExpiringCredentialRefreshingLogin actualInstance = (ExpiringCredentialRefreshingLogin) getField(oauthBearerRefreshingLogin, "expiringCredentialRefreshingLogin");
        ExpiringCredentialRefreshingLogin spyInstance = spy(actualInstance);
        setField(oauthBearerRefreshingLogin, "expiringCredentialRefreshingLogin", spyInstance);

        String serviceName = "test-service";
        // Mock the serviceName() call on the spy to return a predefined service name
        when(spyInstance.serviceName()).thenReturn(serviceName);

        // Retrieve the service name and assert it matches the mocked one
        String retrievedServiceName = oauthBearerRefreshingLogin.serviceName();

        assertEquals(serviceName, retrievedServiceName, "Service name should be delegated correctly from the internal login instance");
        verify(spyInstance).serviceName(); // Ensure serviceName() was called on the spy
    }

    @Test
    void testServiceNameWhenNotConfigured() {
        // When serviceName() is called before configure, the internal login instance is null.
        // The method should return null.
        String retrievedServiceName = oauthBearerRefreshingLogin.serviceName();
        assertNull(retrievedServiceName, "Service name should be null when the login is not configured");
    }

    @Test
    void testLoginWhenConfigured() throws Exception {
        // Configure the login object and spy on its internal instance
        oauthBearerRefreshingLogin.configure(mockConfigs, contextName, mockConfiguration, mockLoginCallbackHandler);
        ExpiringCredentialRefreshingLogin actualInstance = (ExpiringCredentialRefreshingLogin) getField(oauthBearerRefreshingLogin, "expiringCredentialRefreshingLogin");
        ExpiringCredentialRefreshingLogin spyInstance = spy(actualInstance);
        setField(oauthBearerRefreshingLogin, "expiringCredentialRefreshingLogin", spyInstance);

        LoginContext mockLoginContext = mock(LoginContext.class);
        // Mock the login() call on the spy to return a mock LoginContext.
        // Use doReturn().when() for spies to avoid calling the real method, which has side effects.
        doReturn(mockLoginContext).when(spyInstance).login();

        // Perform the login and assert the returned LoginContext matches the mocked one
        LoginContext retrievedLoginContext = oauthBearerRefreshingLogin.login();

        assertEquals(mockLoginContext, retrievedLoginContext, "LoginContext should be delegated correctly from the internal login instance");
        verify(spyInstance).login(); // Ensure login() was called on the spy
    }

    @Test
    void testLoginWhenNotConfiguredThrowsLoginException() {
        // When login() is called before configure, the internal login instance is null.
        // This should result in a LoginException being thrown.
        LoginException thrown = assertThrows(LoginException.class, () -> {
            oauthBearerRefreshingLogin.login();
        }, "Should throw LoginException when not configured");

        // Verify the exception message is as expected
        assertEquals("Login was not configured properly", thrown.getMessage(), "LoginException message mismatch");
    }
}