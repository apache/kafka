package org.apache.kafka.common.security.oauthbearer.internals;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredential;
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredentialRefreshingLogin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class OAuthBearerRefreshingLoginTest {

    private OAuthBearerRefreshingLogin oAuthBearerRefreshingLogin;

    @Mock
    private AuthenticateCallbackHandler mockCallbackHandler;

    @Mock
    private Configuration mockConfiguration;

    @Mock
    private Subject mockSubject;

    @Mock
    private OAuthBearerToken mockOAuthBearerToken;

    @Mock
    private LoginContext mockLoginContext;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        oAuthBearerRefreshingLogin = new OAuthBearerRefreshingLogin();
    }

    private Map<String, Object> createDefaultConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, 0.8);
        configs.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, 0.05);
        configs.put(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, (short) 60);
        configs.put(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, (short) 300);
        return configs;
    }

    @Test
    public void testConfigure_InitializesExpiringCredentialRefreshingLogin() {
        Map<String, Object> configs = createDefaultConfigs();

        oAuthBearerRefreshingLogin.configure(configs, "KafkaClient", mockConfiguration, mockCallbackHandler);

        assertDoesNotThrow(() -> {
            java.lang.reflect.Field field = OAuthBearerRefreshingLogin.class.getDeclaredField("expiringCredentialRefreshingLogin");
            field.setAccessible(true);
            assertNotNull(field.get(oAuthBearerRefreshingLogin));
        });
    }

    @Test
    public void testClose_CallsExpiringCredentialRefreshingLoginClose() throws Exception {
        java.lang.reflect.Field field = OAuthBearerRefreshingLogin.class.getDeclaredField("expiringCredentialRefreshingLogin");
        field.setAccessible(true);
        ExpiringCredentialRefreshingLogin mockExpiringLogin = mock(ExpiringCredentialRefreshingLogin.class);
        field.set(oAuthBearerRefreshingLogin, mockExpiringLogin);

        oAuthBearerRefreshingLogin.close();
        verify(mockExpiringLogin, times(1)).close();
    }

    @Test
    public void testClose_NoOpWhenNotConfigured() {
        oAuthBearerRefreshingLogin.close();
    }

    @Test
    public void testSubject_ReturnsSubjectFromExpiringCredentialRefreshingLogin() throws Exception {
        java.lang.reflect.Field field = OAuthBearerRefreshingLogin.class.getDeclaredField("expiringCredentialRefreshingLogin");
        field.setAccessible(true);
        ExpiringCredentialRefreshingLogin mockExpiringLogin = mock(ExpiringCredentialRefreshingLogin.class);
        field.set(oAuthBearerRefreshingLogin, mockExpiringLogin);

        when(mockExpiringLogin.subject()).thenReturn(mockSubject);

        Subject subject = oAuthBearerRefreshingLogin.subject();
        assertEquals(mockSubject, subject);
        verify(mockExpiringLogin, times(1)).subject();
    }

    @Test
    public void testSubject_ReturnsNullWhenNotConfigured() {
        assertNull(oAuthBearerRefreshingLogin.subject());
    }

    @Test
    public void testServiceName_ReturnsServiceNameFromExpiringCredentialRefreshingLogin() throws Exception {
        java.lang.reflect.Field field = OAuthBearerRefreshingLogin.class.getDeclaredField("expiringCredentialRefreshingLogin");
        field.setAccessible(true);
        ExpiringCredentialRefreshingLogin mockExpiringLogin = mock(ExpiringCredentialRefreshingLogin.class);
        field.set(oAuthBearerRefreshingLogin, mockExpiringLogin);

        when(mockExpiringLogin.serviceName()).thenReturn("KafkaClient");

        String serviceName = oAuthBearerRefreshingLogin.serviceName();
        assertEquals("KafkaClient", serviceName);
        verify(mockExpiringLogin, times(1)).serviceName();
    }

    @Test
    public void testServiceName_ReturnsNullWhenNotConfigured() {
        assertNull(oAuthBearerRefreshingLogin.serviceName());
    }

    @Test
    public void testLogin_CallsExpiringCredentialRefreshingLoginLogin() throws LoginException, Exception {
        java.lang.reflect.Field field = OAuthBearerRefreshingLogin.class.getDeclaredField("expiringCredentialRefreshingLogin");
        field.setAccessible(true);
        ExpiringCredentialRefreshingLogin mockExpiringLogin = mock(ExpiringCredentialRefreshingLogin.class);
        field.set(oAuthBearerRefreshingLogin, mockExpiringLogin);

        when(mockExpiringLogin.login()).thenReturn(mockLoginContext);

        LoginContext loginContext = oAuthBearerRefreshingLogin.login();
        assertEquals(mockLoginContext, loginContext);
        verify(mockExpiringLogin, times(1)).login();
    }

    @Test
    public void testLogin_ThrowsLoginExceptionWhenNotConfigured() {
        LoginException thrown = assertThrows(LoginException.class, () -> oAuthBearerRefreshingLogin.login());
        assertEquals("Login was not configured properly", thrown.getMessage());
    }

    @Test
    public void testExpiringCredential_ReturnsNullWhenNoTokens() throws Exception {
        oAuthBearerRefreshingLogin.configure(createDefaultConfigs(), "KafkaClient", mockConfiguration, mockCallbackHandler);

        java.lang.reflect.Field internalLoginField = OAuthBearerRefreshingLogin.class.getDeclaredField("expiringCredentialRefreshingLogin");
        internalLoginField.setAccessible(true);
        ExpiringCredentialRefreshingLogin internalLogin = (ExpiringCredentialRefreshingLogin) internalLoginField.get(oAuthBearerRefreshingLogin);
        assertNotNull(internalLogin, "Internal ExpiringCredentialRefreshingLogin should have been initialized");

        java.lang.reflect.Field subjectField = ExpiringCredentialRefreshingLogin.class.getDeclaredField("subject");
        subjectField.setAccessible(true);
        subjectField.set(internalLogin, mockSubject);

        when(mockSubject.getPrivateCredentials(OAuthBearerToken.class)).thenReturn(Collections.emptySet());

        ExpiringCredential credential = internalLogin.expiringCredential();

        assertNull(credential);
    }

    @Test
    public void testExpiringCredential_ReturnsExpiringCredentialWhenTokenExists() throws Exception {
        oAuthBearerRefreshingLogin.configure(createDefaultConfigs(), "KafkaClient", mockConfiguration, mockCallbackHandler);

        java.lang.reflect.Field internalLoginField = OAuthBearerRefreshingLogin.class.getDeclaredField("expiringCredentialRefreshingLogin");
        internalLoginField.setAccessible(true);
        ExpiringCredentialRefreshingLogin internalLogin = (ExpiringCredentialRefreshingLogin) internalLoginField.get(oAuthBearerRefreshingLogin);
        assertNotNull(internalLogin, "Internal ExpiringCredentialRefreshingLogin should have been initialized");

        java.lang.reflect.Field subjectField = ExpiringCredentialRefreshingLogin.class.getDeclaredField("subject");
        subjectField.setAccessible(true);
        subjectField.set(internalLogin, mockSubject);

        Set<OAuthBearerToken> tokens = new HashSet<>();
        tokens.add(mockOAuthBearerToken);
        when(mockSubject.getPrivateCredentials(OAuthBearerToken.class)).thenReturn(tokens);
        when(mockOAuthBearerToken.principalName()).thenReturn("testUser");
        when(mockOAuthBearerToken.startTimeMs()).thenReturn(1000L);
        when(mockOAuthBearerToken.lifetimeMs()).thenReturn(3600000L);

        ExpiringCredential credential = internalLogin.expiringCredential();

        assertNotNull(credential);
        assertEquals("testUser", credential.principalName());
        assertEquals(1000L, credential.startTimeMs());
        assertEquals(3600000L, credential.expireTimeMs());
        assertNull(credential.absoluteLastRefreshTimeMs());
    }
}
