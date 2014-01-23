package kafka.test;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

/**
 * Helper functions for writing unit tests
 */
public class TestUtils {

    public static File IO_TMP_DIR = new File(System.getProperty("java.io.tmpdir"));

    public static String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public static String DIGITS = "0123456789";
    public static String LETTERS_AND_DIGITS = LETTERS + DIGITS;

    /* A consistent random number generator to make tests repeatable */
    public static final Random seededRandom = new Random(192348092834L);
    public static final Random random = new Random();

    /**
     * Choose a number of random available ports
     */
    public static int[] choosePorts(int count) {
        try {
            ServerSocket[] sockets = new ServerSocket[count];
            int[] ports = new int[count];
            for (int i = 0; i < count; i++) {
                sockets[i] = new ServerSocket(0);
                ports[i] = sockets[i].getLocalPort();
            }
            for (int i = 0; i < count; i++)
                sockets[i].close();
            return ports;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Choose an available port
     */
    public static int choosePort() {
        return choosePorts(1)[0];
    }

    /**
     * Generate an array of random bytes
     * 
     * @param numBytes The size of the array
     */
    public static byte[] randomBytes(int size) {
        byte[] bytes = new byte[size];
        seededRandom.nextBytes(bytes);
        return bytes;
    }

    /**
     * Generate a random string of letters and digits of the given length
     * 
     * @param len The length of the string
     * @return The random string
     */
    public static String randomString(int len) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; i++)
            b.append(LETTERS_AND_DIGITS.charAt(seededRandom.nextInt(LETTERS_AND_DIGITS.length())));
        return b.toString();
    }

}
