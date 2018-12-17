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

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.Random;

/**
 * This class provides simple functionality for encryption and decryption
 * of Strings. The user supplies cleartext and a password to get ciphertext,
 * or the ciphertext and password to get back the cleartext. The caller may
 * optionally specify the salt to use for the encrypting in order to hold
 * the generated ciphertext constant across config regeneration.
 *
 * This class does all the heavy crypto lifting in a safe,
 * crypto-best-practices manner.
 *
 * The ciphertext is encoded in base 64. The format of the raw bytes of the
 * cipertext is as follows:
 * <ol>
 *   <li>[0-2]   Three bytes of a known tag value 'DTE'</li>
 *   <li>[3]     One byte of version number, 0x1 for now</li>
 *   <li>[4-19]  Sixteen bytes of salt/IV for PBKDF2/AES-CBC</li>
 *   <li>[20-51] 32 bytes of HMAC</li>
 *   <li>[52-N]  All remaining bytes are encrypted data</li>
 * </ol>
 *
 * The password is converted to an AES-128 key and a HMAC-SHA-256 key using
 * PBKDF2. This algorithm takes a password, salt, number of rounds, and
 * desired size of resulting data. The salt is sixteen bytes long, randomly
 * generated, and stored as bytes [4-19] of the ciphertext.
 *
 * The first third of the resulting bits are treated as an AES-128 key. The
 * rest of the resulting bits are treated as the key for HMAC-SHA-256.
 * AES/CBC/PKCS5Padding is used to encrypt the data. The IV for CBC is the
 * salt with all bits flipped. We have a design decision here where we could
 * also have generated a unique IV and saved it along with the salt at the
 * beginning of the String. However, this would take yet another 16 bytes of
 * space, and one primary use case of this code is to encrypt small text like
 * passwords. As a result I decided to re-use the salt with an "interesting"
 * transform.
 *
 * Finally, an HMAC-SHA-256 is taken of the encrypted data and stored in
 * bytes 20-51 of the output. This obeys the important encrypt-then-MAC
 * principal. Upon decryption, this HMAC is re-calculated and compared with
 * the value of bits 20-51. If equal, the decryption proceeds as a reverse
 * of the above.
 *
 * The password and cleartext are dealt with as arrays of characters instead
 * of Strings because Strings are immutable and live until garbage collected,
 * and arrays of characters can be cleared immediately after use. The
 * ciphertext is non-sensitive and is handled as a String.
 *
 * Tragically, we can run into java crypto export silliness because of
 * AES-256 if the unlimited strength crypto jar isn't installed. We could
 * detect this condition and fall back, but that would mean that we'd have
 * two different version formats. As a result, we'll stick to AES-128.
 */
public class EncryptUtil {

    /**
     * The number of rounds in which to apply PBKDF2 to the password in order to
     * generate a key. Higher numbers increase the mathematical complexity of
     * brute force breaking the password. This number is a reasonable trade
     * off between complexity and "doesn't make the machine go out to lunch
     * when run in a debugger".
     */
    private static final int PBKDF2_ROUNDS = 4096;

    /**
     * The length, in bytes, of the salt.
     */
    private static final int SALT_LENGTH = 16;

    /**
     * Number of bytes in the HMAC key we're using. 32*8 = 256 bits
     */
    private static final int HMAC_KEY_SIZE = 32;

    /**
     * Number of bytes in the AES key we're using. 16*8 = 128 bits. We aren't
     * using AES-256 because we might not have the unlimited strength crypto
     * installed.
     */
    private static final int AES_KEY_SIZE = 16;

    /**
     * Size of output of HMAC-SHA-256, in bytes.
     */
    private static final int HMAC_SIZE = 32;

    /**
     * PBKDF2 algorithm.
     */
    private static final String PBKDF2_ALGO = "PBKDF2WithHmacSHA256";

    /**
     * Hashing algorithm
     */
    private static final String HASH_ALGO = "SHA-256";

    /**
     * HMAC algorithm - 256 bit key.
     */
    private static final String HMAC_ALGO = "HmacSHA256";

    /**
     * Symmetric cipher
     */
    private static final String CIPHER_ALGO = "AES";

    /**
     * AES with Cipher Block Chaining and padding.
     */
    private static final String CIPHER_SPEC = "AES/CBC/PKCS5Padding";

    /**
     * Current version of the crypto used by this class. If any of the
     * crypto or crypto parameters changes in the future, bump up this
     * number and have the decrypt routine handle both old and new versions.
     */
    private static final byte VERSION_1 = 0x1;

    /**
     * Randomly generate a salt value
     * @return A newly allocated byte array of SALT_LENGTH containing random values.
     */
    private static byte[] genSalt() {
        byte[] salt = new byte[SALT_LENGTH];
        new Random().nextBytes(salt);
        return salt;
    }

    /**
     * Generate a salt value from a String passed in from the user. This
     * implementation takes the first SALT_LENGTH bytes of a SHA-256 hash
     * of the string.
     * @param saltString String upon which to base the bytes of salt
     * @return First SALT_LENGTH bytes of SHA-256 hash of saltString
     * @throws GeneralSecurityException
     * @throws UnsupportedEncodingException
     */
    private static byte[] genSaltFromString(String saltString)
            throws GeneralSecurityException, UnsupportedEncodingException {
        MessageDigest digest = MessageDigest.getInstance(HASH_ALGO);
        byte[] sha256 = digest.digest(saltString.getBytes("UTF-8"));
        byte[] salt = new byte[SALT_LENGTH];
        System.arraycopy(sha256, 0, salt, 0, SALT_LENGTH);
        return salt;
    }

    /**
     * Given a salt, flip all the bits to make the IV
     * @param salt The salt
     * @return A newly allocated byte array of SALT_LENGTH containing a suitable IV.
     */
    private static byte[] getIV(byte[] salt) {
        byte[] IV = new byte[SALT_LENGTH];
        for (int i = 0; i < SALT_LENGTH; i++) {
            IV[i] = (byte) ~salt[i];
        }
        return IV;
    }

    /**
     * Returns a byte array containing both the AES-256 key (first half) and the
     * HMAC-SHA-256 key (second half). This is done using PBKDF2, which takes as
     * input the password and salt, and returns a very-computationally-difficult
     * hash.
     * @param password Input password for hashing
     * @param salt Input salt for hashing
     * @return 64 bytes, first half is the AES-256 key, second half is the
     *         HMAC-SHA-256 key
     * @throws GeneralSecurityException
     */
    private static byte[] getKeys(char[] password, byte[] salt)
            throws GeneralSecurityException {

        PBEKeySpec spec = new PBEKeySpec(password, salt, PBKDF2_ROUNDS,
                (AES_KEY_SIZE + HMAC_KEY_SIZE) * 8 * 2);
        byte[] key;
        SecretKeyFactory skf = SecretKeyFactory.getInstance(PBKDF2_ALGO);
        key = skf.generateSecret(spec).getEncoded();
        return key;
    }

    /**
     * Used by the byte-based cleanse() below, a counter to help populate
     * the cleared bytes with garbage.
     */
    private static byte clearByteCtr = 0x17;

    /**
     * Overwrite the given byte array with garbage. Similar in flavor to
     * OPENSSL_cleanse().
     * @param bytes Byte array to overwrite.
     */
    public static void cleanse(byte[] bytes) {
        if (bytes == null) {
            return;
        }
        for (int i = 0; i < bytes.length; i++) {
            clearByteCtr ^= bytes[i];
            bytes[i] = clearByteCtr;
        }
    }

    /**
     * Used by the char-based clease() below, a counter to help populate
     * the cleared bytes with garbage.
     */
    private static char clearCharCtr = 'X';

    /**
     * Overwrite the given char array with garbage. Similar in flavor to
     * OPENSSL_cleanse().
     * @param chars Char array to overwrite.
     */
    public static void cleanse(char[] chars) {
        if (chars == null) {
            return;
        }
        for (int i = 0; i < chars.length; i++) {
            clearCharCtr ^= chars[i];
            chars[i] = clearCharCtr;
        }
    }

    /**
     * Given a password and some cleartext to encrypt, return a String
     * containing the resulting ciphertext. Thread safe.
     *
     * @param password Password to encrypt the cleartext with
     * @param cleartext The data to encrypt
     * @return Resulting ciphertext
     * @throws GeneralSecurityException
     * @throws UnsupportedEncodingException
     */
    public static byte[] encrypt(char[] password, byte[] cleartext)
            throws GeneralSecurityException {
        byte[] salt = genSalt();
        try {
            return encrypt(password, salt, cleartext);
        } finally {
            cleanse(salt);
        }
    }

    /**
     * Given a password, a String of salt, and some cleartext to encrypt,
     * return a String containing the resulting ciphertext. Thread safe.
     *
     * @param password Password to encrypt the cleartext with
     * @param saltString A string with which to base bytes of salt
     * @param cleartext The data to encrypt
     * @return Resulting ciphertext
     * @throws GeneralSecurityException
     * @throws UnsupportedEncodingException
     */
    public static byte[] encrypt(char[] password, String saltString, byte[] cleartext)
            throws GeneralSecurityException, UnsupportedEncodingException {
        byte[] salt = genSaltFromString(saltString);
        try {
            return encrypt(password, salt, cleartext);
        } finally {
            cleanse(salt);
        }
    }

    /**
     * Internal implementation of encrypt() given the bytes for password,
     * salt, and cleartext.
     * Implementation details: See class documentation and inline comments.
     *
     * @param password Password to encrypt the cleartext with
     * @param salt SALT_LENGTH bytes of salt for encryption
     * @param clearbytes The data to encrypt
     * @return Resulting ciphertext in a byte array
     */
     static byte[] encrypt(char[] password, byte[] salt, byte[] clearbytes)
            throws GeneralSecurityException {
        Objects.requireNonNull(password);
        Objects.requireNonNull(clearbytes);
        if (password.length <= 0) {
            throw new IllegalArgumentException("Illegal password length");
        }
        if (clearbytes.length <= 0) {
            throw new IllegalArgumentException("Illegal text length");
        }
        if (salt.length != SALT_LENGTH) {
            throw new IllegalStateException("Illegal salt length");
        }

        byte[] IV = null;
        byte[] keyBytes = null;
        byte[] cipherBytes = null;
        byte[] hmacBytes = null;
        byte[] outputBytes = null;

        try {
            // Start out getting a fresh IV
            IV = getIV(salt);

            // Create keys from the password and salt
            keyBytes = getKeys(password, salt);
            SecretKey aesKey = new SecretKeySpec(keyBytes, 0, AES_KEY_SIZE, CIPHER_ALGO);
            SecretKey hmacKey = new SecretKeySpec(keyBytes, AES_KEY_SIZE, HMAC_KEY_SIZE, HMAC_ALGO);

            // Encrypt the cleartext using aesKey and IV
            Cipher cipher = Cipher.getInstance(CIPHER_SPEC);
            cipher.init(Cipher.ENCRYPT_MODE, aesKey, new IvParameterSpec(IV));
            cipherBytes = cipher.doFinal(clearbytes);

            // HMAC the ciphertext using hmacKey
            Mac hmac = Mac.getInstance(HMAC_ALGO);
            hmac.init(hmacKey);
            hmacBytes = hmac.doFinal(cipherBytes);

            // Piece together the output bytes in a known format
            outputBytes = new byte[4 + salt.length + hmacBytes.length + cipherBytes.length];
            outputBytes[0] = 'D';
            outputBytes[1] = 'T';
            outputBytes[2] = 'E';
            outputBytes[3] = VERSION_1;
            System.arraycopy(salt, 0, outputBytes, 4, salt.length);
            System.arraycopy(hmacBytes, 0, outputBytes, 4 + salt.length, hmacBytes.length);
            System.arraycopy(cipherBytes, 0, outputBytes, 4 + salt.length +
                    hmacBytes.length, cipherBytes.length);
            // Base 64 is the safest String output representation for bytes.
            outputBytes = Base64.getEncoder().encode(outputBytes);
        } finally {       // Tidy up intermediate values.
            cleanse(IV);
            cleanse(keyBytes);
            cleanse(cipherBytes);
            cleanse(hmacBytes);
        }

        return outputBytes;
    }

    /**
     * Given a byte array of ciphertext encrypted with the above function, decrypt
     * it using the password and return the resulting cleartext. Thread safe.
     *
     * Implementation details: See class documentation and inline comments.
     *
     * @param password Password to use to decrypt.
     * @param cipherbytes String encrypted with encrypt() above.
     * @return plaintext
     */
    public static byte[] decrypt(char[] password, byte[] cipherbytes)
            throws GeneralSecurityException {
        Objects.requireNonNull(password);
        Objects.requireNonNull(cipherbytes);
        if (password.length <= 0) {
            throw new IllegalArgumentException("Illegal password length");
        }
        if (cipherbytes.length == 0) {
            throw new IllegalArgumentException("Illegal cipher text (empty)");
        }
        cipherbytes = Base64.getDecoder().decode(cipherbytes);
        byte[] inputBytes = null;
        byte[] salt = null;
        byte[] hmacBytes = null;
        byte[] cipherBytes = null;
        byte[] IV = null;
        byte[] keyBytes = null;
        byte[] calculatedHmacBytes = null;
        byte[] plainBytes = null;

        try {
            // Back to bytes for all calculations.
            inputBytes = cipherbytes;

            if (inputBytes.length == 0) {
                throw new BadPaddingException("Input ciphertext not base 64");
            }

            // Check for length of input below tag size
            if (inputBytes.length < 4) {
                throw new BadPaddingException("Input ciphertext much too short");
            }

            if (inputBytes.length < (4 + SALT_LENGTH + HMAC_SIZE)) {
                throw new BadPaddingException("Input ciphertext too short");
            }

            // Verify known tag
            if ((inputBytes[0] != 'D') ||
                    (inputBytes[1] != 'T') ||
                    (inputBytes[2] != 'E')) {
                throw new BadPaddingException("Invalid Ciphertext String");
            }

            // Verify the version.
            if (inputBytes[3] != 0x1) {
                throw new BadPaddingException("Invalid Ciphertext Version");
            }

            // Extract the various components of the input into respective items.
            salt = new byte[SALT_LENGTH];
            hmacBytes = new byte[HMAC_SIZE];
            cipherBytes = new byte[inputBytes.length - (4 + salt.length + hmacBytes.length)];
            System.arraycopy(inputBytes, 4, salt, 0, salt.length);
            System.arraycopy(inputBytes, 4 + salt.length, hmacBytes, 0, hmacBytes.length);
            System.arraycopy(inputBytes, 4 + salt.length + hmacBytes.length,
                    cipherBytes, 0, cipherBytes.length);
            IV = getIV(salt);

            // Create keys from the password and salt
            keyBytes = getKeys(password, salt);
            SecretKey aesKey = new SecretKeySpec(keyBytes, 0, AES_KEY_SIZE, CIPHER_ALGO);
            SecretKey hmacKey = new SecretKeySpec(keyBytes, AES_KEY_SIZE, HMAC_KEY_SIZE, HMAC_ALGO);

            // Perform an HMAC of the cipher bytes, and throw an exception if they
            // don't match. This indicates corruption or tampering.
            Mac hmac = Mac.getInstance(HMAC_ALGO);
            hmac.init(hmacKey);
            calculatedHmacBytes = hmac.doFinal(cipherBytes);
            if (!Arrays.equals(calculatedHmacBytes, hmacBytes)) {
                throw new BadPaddingException("Detected ciphertext tampering");
            }

            // Decrypt the cipher bytes using aesKey and IV
            Cipher cipher = Cipher.getInstance(CIPHER_SPEC);
            cipher.init(Cipher.DECRYPT_MODE, aesKey, new IvParameterSpec(IV));
            plainBytes = cipher.doFinal(cipherBytes);

        } finally {  // Tidy up intermediate values.
            cleanse(inputBytes);
            cleanse(salt);
            cleanse(hmacBytes);
            cleanse(cipherBytes);
            cleanse(IV);
            cleanse(calculatedHmacBytes);
            cleanse(keyBytes);
        }

        return plainBytes;
    }
}
