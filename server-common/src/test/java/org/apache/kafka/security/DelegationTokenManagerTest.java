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
package org.apache.kafka.security;

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.server.config.DelegationTokenManagerConfigs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.Mac;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DelegationTokenManagerTest {
    @Mock
    private AbstractConfig mockConfig;
    
    @Mock
    private Mac mac;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    public void testTokenAuthDisabledEmptySecretKey() {
        Password password = new Password("");
        when(mockConfig.getPassword(anyString())).thenReturn(password);
        when(mockConfig.getLong(anyString())).thenReturn(1L);
        DelegationTokenManagerConfigs delegationTokenManagerConfigs = new DelegationTokenManagerConfigs(mockConfig);
        
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(ScramMechanism.SCRAM_SHA_256.mechanismName()));
        
        DelegationTokenManager delegationTokenManager = new DelegationTokenManager(delegationTokenManagerConfigs, delegationTokenCache);
        assertFalse(delegationTokenManager.isEnabled());
    }
    
    @Test
    public void testTokenAuthDisabledSecretKeyNotConfigured() {
        when(mockConfig.getPassword(anyString())).thenReturn(null);
        when(mockConfig.getLong(anyString())).thenReturn(1L);
        DelegationTokenManagerConfigs delegationTokenManagerConfigs = new DelegationTokenManagerConfigs(mockConfig);
        
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(ScramMechanism.SCRAM_SHA_256.mechanismName()));
        
        DelegationTokenManager delegationTokenManager = new DelegationTokenManager(delegationTokenManagerConfigs, delegationTokenCache);
        assertFalse(delegationTokenManager.isEnabled());
    }
    
    @Test
    public void testTokenAuthEnabledSecretKey() {
    	Password password = new Password("password");
        when(mockConfig.getPassword(anyString())).thenReturn(password);
        when(mockConfig.getLong(anyString())).thenReturn(1L);
        DelegationTokenManagerConfigs delegationTokenManagerConfigs = new DelegationTokenManagerConfigs(mockConfig);
        
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(ScramMechanism.SCRAM_SHA_256.mechanismName()));
        
        DelegationTokenManager delegationTokenManager = new DelegationTokenManager(delegationTokenManagerConfigs, delegationTokenCache);
        assertTrue(delegationTokenManager.isEnabled());
    }
    
    @Test
    public void testTokenAuthEnabledSecretKeyLength0() {
    	Password password = new Password("");

    	DelegationTokenManagerConfigs delegationTokenManagerConfigs = mock(DelegationTokenManagerConfigs.class);
        when(delegationTokenManagerConfigs.tokenAuthEnabled()).thenReturn(true);
        when(delegationTokenManagerConfigs.delegationTokenSecretKey()).thenReturn(password);
        
        DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(ScramMechanism.SCRAM_SHA_256.mechanismName()));
        
        DelegationTokenManager delegationTokenManager = new DelegationTokenManager(delegationTokenManagerConfigs, delegationTokenCache);
        assertFalse(delegationTokenManager.isEnabled());
    }
    
    @Test
    public void testGetDelegationToken() throws InvalidKeyException {
    	try(MockedStatic<Mac> mockedStaticMac = mockStatic(Mac.class)){
            Password password = new Password("password");
            when(mockConfig.getPassword(anyString())).thenReturn(password);
            when(mockConfig.getLong(anyString())).thenReturn(1L);
            DelegationTokenManagerConfigs delegationTokenManagerConfigs = new DelegationTokenManagerConfigs(mockConfig);
        
            DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(ScramMechanism.SCRAM_SHA_256.mechanismName()));
            
            mockedStaticMac.when(() -> Mac.getInstance(anyString())).thenReturn(mac);
            doNothing().when(mac).init(any());
            when(mac.doFinal(any())).thenReturn(new byte[] {1});
            
            DelegationTokenManager delegationTokenManager = new DelegationTokenManager(delegationTokenManagerConfigs, delegationTokenCache);
            KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Name1");
            List<KafkaPrincipal> principals = new ArrayList<>();
            principals.add(principal);
            
            TokenInformation tokenInformation = new TokenInformation("1", principal, principals, 1L, 1L, 1L);
            
            DelegationToken token = delegationTokenManager.getDelegationToken(tokenInformation);
            assertNotNull(token);
            assertSame(tokenInformation, token.tokenInfo());
        }
    }
    
    @Test
    public void testGetDelegationTokenThrowsIllegalArgumentException() throws InvalidKeyException {
    	try(MockedStatic<Mac> mockedStaticMac = mockStatic(Mac.class)){
            Password password = new Password("password");
            when(mockConfig.getPassword(anyString())).thenReturn(password);
            when(mockConfig.getLong(anyString())).thenReturn(1L);
            DelegationTokenManagerConfigs delegationTokenManagerConfigs = new DelegationTokenManagerConfigs(mockConfig);
        
            DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(ScramMechanism.SCRAM_SHA_256.mechanismName()));
            
            mockedStaticMac.when(() -> Mac.getInstance(anyString())).thenReturn(mac);
            doThrow(InvalidKeyException.class).when(mac).init(any());
            when(mac.doFinal(any())).thenReturn(new byte[] {1});
            
            DelegationTokenManager delegationTokenManager = new DelegationTokenManager(delegationTokenManagerConfigs, delegationTokenCache);
            KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Name1");
            List<KafkaPrincipal> principals = new ArrayList<>();
            principals.add(principal);
            
            TokenInformation tokenInformation = new TokenInformation("1", principal, principals, 1L, 1L, 1L);
            
            assertThrows(IllegalArgumentException.class, () -> delegationTokenManager.getDelegationToken(tokenInformation));
        }
    }
    
    @Test
    public void testGetDelegationTokenThrowsRuntimeException() throws InvalidKeyException {
    	try(MockedStatic<Mac> mockedStaticMac = mockStatic(Mac.class)){
            Password password = new Password("password");
            when(mockConfig.getPassword(anyString())).thenReturn(password);
            when(mockConfig.getLong(anyString())).thenReturn(1L);
            DelegationTokenManagerConfigs delegationTokenManagerConfigs = new DelegationTokenManagerConfigs(mockConfig);
        
            DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(ScramMechanism.SCRAM_SHA_256.mechanismName()));
            
            mockedStaticMac.when(() -> Mac.getInstance(anyString())).thenReturn(mac);
            doNothing().when(mac).init(any());
            when(mac.doFinal(any())).thenThrow(RuntimeException.class);
            
            DelegationTokenManager delegationTokenManager = new DelegationTokenManager(delegationTokenManagerConfigs, delegationTokenCache);
            KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Name1");
            List<KafkaPrincipal> principals = new ArrayList<>();
            principals.add(principal);
            
            TokenInformation tokenInformation = new TokenInformation("1", principal, principals, 1L, 1L, 1L);
            
            assertThrows(RuntimeException.class, () -> delegationTokenManager.getDelegationToken(tokenInformation));
        }
    }
    
    @Test
    public void testUpdateToken() throws NoSuchAlgorithmException {
    	KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Name1");
        List<KafkaPrincipal> principals = new ArrayList<>();
        principals.add(principal);
        
        TokenInformation tokenInformation = new TokenInformation("1", principal, principals, 1L, 1L, 1L);
    	DelegationToken delegationToken = new DelegationToken(tokenInformation, "Password".getBytes());
    	ScramCredential scramCredential = new ScramCredential(new byte[] {0}, new byte[] {1}, new byte[] {2}, 4096);
    	
    	try(MockedConstruction<ScramFormatter> mockScramFormatterConstruction = mockConstruction(ScramFormatter.class, (mock, context) -> {
    		when(mock.generateCredential(anyString(), anyInt())).thenReturn(scramCredential);
    	})){
    		Password password = new Password("password");
            when(mockConfig.getPassword(anyString())).thenReturn(password);
            when(mockConfig.getLong(anyString())).thenReturn(1L);
            DelegationTokenManagerConfigs delegationTokenManagerConfigs = new DelegationTokenManagerConfigs(mockConfig);
        
            DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(ScramMechanism.SCRAM_SHA_256.mechanismName()));
            DelegationTokenManager delegationTokenManager = new DelegationTokenManager(delegationTokenManagerConfigs, delegationTokenCache);
            delegationTokenManager.updateToken(delegationToken);
            assertSame(delegationTokenCache.token("1"), tokenInformation);
    	}
    }
    
    @Test
    public void testUpdateTokenKeepsUserIndependent() throws NoSuchAlgorithmException {
    	KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Name1");
        List<KafkaPrincipal> principals = new ArrayList<>();
        principals.add(principal);
        
        TokenInformation tokenInformation = new TokenInformation("1", principal, principals, 1L, 1L, 1L);
    	DelegationToken delegationToken = new DelegationToken(tokenInformation, "Password".getBytes());
    	
    	TokenInformation tokenInformation1 = new TokenInformation("12", principal, principals, 1L, 1L, 1L);
    	DelegationToken delegationToken1 = new DelegationToken(tokenInformation1, "Password".getBytes());
    	ScramCredential scramCredential = new ScramCredential(new byte[] {0}, new byte[] {1}, new byte[] {2}, 4096);
    	
    	try(MockedConstruction<ScramFormatter> mockScramFormatterConstruction = mockConstruction(ScramFormatter.class, (mock, context) -> {
    		when(mock.generateCredential(anyString(), anyInt())).thenReturn(scramCredential);
    	})){
    		Password password = new Password("password");
            when(mockConfig.getPassword(anyString())).thenReturn(password);
            when(mockConfig.getLong(anyString())).thenReturn(1L);
            DelegationTokenManagerConfigs delegationTokenManagerConfigs = new DelegationTokenManagerConfigs(mockConfig);
        
            DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(ScramMechanism.SCRAM_SHA_256.mechanismName()));
            DelegationTokenManager delegationTokenManager = new DelegationTokenManager(delegationTokenManagerConfigs, delegationTokenCache);
            delegationTokenManager.updateToken(delegationToken);
            delegationTokenManager.updateToken(delegationToken1);
            assertSame(delegationTokenCache.token("1"), tokenInformation);
            assertSame(delegationTokenCache.token("12"), tokenInformation1);
            assertNotSame(delegationTokenCache.token("1"), delegationTokenCache.token("12"));
    	}
    }
    
    @Test
    public void testRemoveToken() throws NoSuchAlgorithmException {
    	KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Name1");
        List<KafkaPrincipal> principals = new ArrayList<>();
        principals.add(principal);
        
        TokenInformation tokenInformation = new TokenInformation("1", principal, principals, 1L, 1L, 1L);
    	DelegationToken delegationToken = new DelegationToken(tokenInformation, "Password".getBytes());
    	ScramCredential scramCredential = new ScramCredential(new byte[] {0}, new byte[] {1}, new byte[] {2}, 4096);
    	
    	try(MockedConstruction<ScramFormatter> mockScramFormatterConstruction = mockConstruction(ScramFormatter.class, (mock, context) -> {
    		when(mock.generateCredential(anyString(), anyInt())).thenReturn(scramCredential);
    	})){
    		Password password = new Password("password");
            when(mockConfig.getPassword(anyString())).thenReturn(password);
            when(mockConfig.getLong(anyString())).thenReturn(1L);
            DelegationTokenManagerConfigs delegationTokenManagerConfigs = new DelegationTokenManagerConfigs(mockConfig);
        
            DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(ScramMechanism.SCRAM_SHA_256.mechanismName()));
            DelegationTokenManager delegationTokenManager = new DelegationTokenManager(delegationTokenManagerConfigs, delegationTokenCache);
            delegationTokenManager.updateToken(delegationToken);
            assertSame(delegationTokenCache.token("1"), tokenInformation);
            
            delegationTokenManager.removeToken("1");
            assertNull(delegationTokenCache.token("1"));
    	}
    }
    
    @Test
    public void testRemoveTokenKeepsUserIndependent() throws NoSuchAlgorithmException {
    	KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Name1");
        List<KafkaPrincipal> principals = new ArrayList<>();
        principals.add(principal);
        
        TokenInformation tokenInformation = new TokenInformation("1", principal, principals, 1L, 1L, 1L);
    	DelegationToken delegationToken = new DelegationToken(tokenInformation, "Password".getBytes());
    	
    	TokenInformation tokenInformation1 = new TokenInformation("12", principal, principals, 1L, 1L, 1L);
    	DelegationToken delegationToken1 = new DelegationToken(tokenInformation1, "Password".getBytes());
    	ScramCredential scramCredential = new ScramCredential(new byte[] {0}, new byte[] {1}, new byte[] {2}, 4096);
    	
    	try(MockedConstruction<ScramFormatter> mockScramFormatterConstruction = mockConstruction(ScramFormatter.class, (mock, context) -> {
    		when(mock.generateCredential(anyString(), anyInt())).thenReturn(scramCredential);
    	})){
    		Password password = new Password("password");
            when(mockConfig.getPassword(anyString())).thenReturn(password);
            when(mockConfig.getLong(anyString())).thenReturn(1L);
            DelegationTokenManagerConfigs delegationTokenManagerConfigs = new DelegationTokenManagerConfigs(mockConfig);
        
            DelegationTokenCache delegationTokenCache = new DelegationTokenCache(List.of(ScramMechanism.SCRAM_SHA_256.mechanismName()));
            DelegationTokenManager delegationTokenManager = new DelegationTokenManager(delegationTokenManagerConfigs, delegationTokenCache);
            delegationTokenManager.updateToken(delegationToken);
            delegationTokenManager.updateToken(delegationToken1);
            assertSame(delegationTokenCache.token("1"), tokenInformation);
            assertSame(delegationTokenCache.token("12"), tokenInformation1);
            
            assertNotSame(delegationTokenCache.token("1"), delegationTokenCache.token("12"));
            delegationTokenManager.removeToken("1");
            assertNull(delegationTokenCache.token("1"));
            assertNotNull(delegationTokenCache.token("12"));
    	}
    }
}
