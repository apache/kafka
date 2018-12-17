/**
 * Copyright (c) 2018 Cloudera, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import javax.crypto.BadPaddingException;
import java.util.Arrays;
import java.util.Base64;

public class EncryptUtilTest {

    private void testSuccess(char[] password, byte[] cleartext) throws Exception {
        byte[] ciphertext = EncryptUtil.encrypt(password, cleartext);
        byte[] decrypted = EncryptUtil.decrypt(password, ciphertext);
        Assert.assertArrayEquals(cleartext, decrypted);
        Assert.assertFalse(new String(ciphertext).contains(String.valueOf(cleartext)));
        Assert.assertFalse(new String(ciphertext).contains(String.valueOf(password)));
        Assert.assertFalse(new String(ciphertext).contains("\n"));
        EncryptUtil.cleanse(password);
        EncryptUtil.cleanse(cleartext);
    }

    private byte[] corruptOneByte(byte[] cipherText, int bytePos) {
        byte[] cipherBytes = Base64.getDecoder().decode(cipherText);
        cipherBytes[bytePos] ^= 0xff;
        return Base64.getEncoder().encode(cipherBytes);
    }

    private void testCorruption(int pos, String exeptionString)
            throws Exception {
        byte[] cleartext = "Hello, world".getBytes();
        char[] password = "password".toCharArray();
        byte[] ciphertext = EncryptUtil.encrypt(password, cleartext);
        byte[] badCipherText = corruptOneByte(ciphertext, pos);
        try {
            EncryptUtil.decrypt(password, badCipherText);
            Assert.fail("EncryptUtil.decrypt didn't throw expected exception");
        } catch (BadPaddingException bpe) {
            Assert.assertTrue(bpe.getMessage().contains(exeptionString));
        } finally {
            EncryptUtil.cleanse(cleartext);
        }
    }

    @Test
    public void testSimple() throws Exception {
        byte[] cleartext = "Hello, world".getBytes();
        char[] password = "password".toCharArray();
        testSuccess(password, cleartext);
    }

    @Test
    public void testLongPassword() throws Exception {
        byte[] cleartext = "Goodbye, cruel world.".getBytes();
        char[] password = new char[1024];
        Arrays.fill(password, 'a');
        testSuccess(password, cleartext);
    }

    @Test
    public void testLongText() throws Exception {
        byte[] cleartext = new byte[2048];
        Arrays.fill(cleartext, "b".getBytes()[0]);
        char[] password = "SuperSecretPassw0rd".toCharArray();
        testSuccess(password, cleartext);
    }

    @Test
    public void testOneChar() throws Exception {
        byte[] cleartext = "{".getBytes();  // Not in base64 character set
        char[] password = "}".toCharArray();   // Not in base64 character set
        testSuccess(password, cleartext);
    }

    @Test
    public void testRandom() throws Exception {
        String password;
        String cleartext;
        for (int i = 0; i < 100 ; i++) {
            password = RandomStringUtils.random(20);
            cleartext = RandomStringUtils.random(100);
            testSuccess(password.toCharArray(), cleartext.getBytes());
        }
    }

    @Test
    public void testBadTag() throws Exception {
        testCorruption(0, "Invalid Ciphertext String");
    }

    @Test
    public void testBadVersion() throws Exception {
        testCorruption(3, "Invalid Ciphertext Version");
    }

    @Test
    public void testBadSalt() throws Exception {
        testCorruption(10, "Detected ciphertext tampering");
    }

    @Test
    public void testBadHmac() throws Exception {
        testCorruption(25, "Detected ciphertext tampering");
    }

    @Test
    public void testCorruption() throws Exception {
        testCorruption(55, "Detected ciphertext tampering");
    }

    @Test
    public void testNotBase64() throws Exception {
        try {
            EncryptUtil.decrypt("pass".toCharArray(),
                    "{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{".getBytes());
            Assert.fail("EncryptUtil.decrypt didn't throw non-base 64 exception");
        } catch (IllegalArgumentException bpe) {
            Assert.assertTrue(bpe.getMessage().contains("Illegal base64"));
        }
    }

    @Test
    public void testMuchTooShort() throws Exception {
        try {
            EncryptUtil.decrypt("pass".toCharArray(), "Q01".getBytes());
            Assert.fail("EncryptUtil.decrypt didn't throw 'much too short' exception");
        } catch (BadPaddingException bpe) {
            Assert.assertTrue(bpe.getMessage().contains("Input ciphertext much too short"));
        }
    }

    @Test
    public void testTooShort() throws Exception {
        try {
            EncryptUtil.decrypt("pass".toCharArray(), "Q01FAVu4rUjvcgrx".getBytes());
            Assert.fail("EncryptUtil.decrypt didn't throw 'too short' exception");
        } catch (BadPaddingException bpe) {
            Assert.assertTrue(bpe.getMessage().contains("Input ciphertext too short"));
        }
    }

    @Test
    public void testPlainCiphertext() throws Exception {
        // This text from a real example
        byte[] text = ("smtp://send-mail-1.test.com:25?from=noreply" +
                "%40localhost&to=cca-rh65-1%40receive-mail-1.test.com&" +
                "username=test@send-mail-1.test.com&password=password").getBytes();
        try {
            EncryptUtil.decrypt("pass".toCharArray(), text);
            Assert.fail("EncryptUtil.decrypt unexpectedly decrypted plaintext");
        } catch (IllegalArgumentException bpe) {
            Assert.assertTrue(bpe.getMessage().contains("Illegal base64"));
        }
    }

    @Test
    public void testCleanseBytes() {
        byte[] bytes = "This is a somewhat long string".getBytes();
        byte[] original = Arrays.copyOf(bytes, bytes.length);
        for (int i = 0; i < 1000; i++) {
            byte[] previous = Arrays.copyOf(bytes, bytes.length);
            EncryptUtil.cleanse(bytes);
            Assert.assertFalse(Arrays.equals(bytes, previous));
            Assert.assertFalse(Arrays.equals(bytes, original));
        }
    }

    @Test
    public void testCleanseChars() {
        char[] chars = "This is a somewhat long string".toCharArray();
        char[] original = Arrays.copyOf(chars, chars.length);
        for (int i = 0; i < 1000; i++) {
            char[] previous = Arrays.copyOf(chars, chars.length);
            EncryptUtil.cleanse(chars);
            Assert.assertFalse(Arrays.equals(chars, previous));
            Assert.assertFalse(Arrays.equals(chars, original));
        }
    }

    @Test
    public void testSalt() throws Exception {
        byte[] chars = "This is self-referential plaintext".getBytes();
        String salt1 = "I am salty";
        String salt2 = "I like pepper";
        char[] password = "This is a password".toCharArray();
        byte[] ciphertext1a = EncryptUtil.encrypt(password, salt1, chars);
        byte[] ciphertext2 = EncryptUtil.encrypt(password, salt2, chars);
        byte[] ciphertext3a = EncryptUtil.encrypt(password, chars);
        byte[] ciphertext3b = EncryptUtil.encrypt(password, chars);

        Assert.assertNotEquals(ciphertext1a, ciphertext2);
        Assert.assertNotEquals(ciphertext1a, ciphertext3a);
        Assert.assertNotEquals(ciphertext1a, ciphertext3a);
        Assert.assertNotEquals(ciphertext3a, ciphertext3b);
    }
}