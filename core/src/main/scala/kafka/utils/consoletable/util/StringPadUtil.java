package kafka.utils.consoletable.util;

import java.nio.charset.Charset;

public class StringPadUtil {

    public static String leftPad(String str, int size, char c) {
        if (str == null) {
            return null;
        }

        int strLength = strLength(str);
        if (size <= 0 || size <= strLength) {
            return str;
        }
        return repeat(size - strLength, c).concat(str);
    }

    public static String rightPad(String str, int size, char c) {
        if (str == null) {
            return null;
        }

        int strLength = strLength(str);
        if (size <= 0 || size <= strLength) {
            return str;
        }
        return str.concat(repeat(size - strLength, c));
    }

    public static String center(String str, int size, char c) {
        if (str == null) {
            return null;
        }

        int strLength = strLength(str);
        if (size <= 0 || size <= strLength) {
            return str;
        }
        str = leftPad(str, strLength + (size - strLength) / 2, c);
        str = rightPad(str, size, c);
        return str;
    }

    public static String leftPad(String str, int size) {
        return leftPad(str, size, ' ');
    }

    public static String rightPad(String str, int size) {
        return rightPad(str, size, ' ');
    }

    public static String center(String str, int size) {
        return center(str, size, ' ');
    }

    private static String repeat(int size, char c) {
        StringBuilder s = new StringBuilder();
        for (int index = 0; index < size; index++) {
            s.append(c);
        }
        return s.toString();
    }

    public static int strLength(String str) {
        return strLength(str, "UTF-8");
    }

    public static int strLength(String str, String charset) {
        int len = 0;
        int j = 0;
        byte[] bytes = str.getBytes(Charset.forName(charset));
        while (bytes.length > 0) {
            short tmpst = (short) (bytes[j] & 0xF0);
            if (tmpst >= 0xB0) {
                if (tmpst < 0xC0) {
                    j += 2;
                    len += 2;
                } else if ((tmpst == 0xC0) || (tmpst == 0xD0)) {
                    j += 2;
                    len += 2;
                } else if (tmpst == 0xE0) {
                    j += 3;
                    len += 2;
                } else if (tmpst == 0xF0) {
                    short tmpst0 = (short) (((short) bytes[j]) & 0x0F);
                    if (tmpst0 == 0) {
                        j += 4;
                        len += 2;
                    } else if ((tmpst0 > 0) && (tmpst0 < 12)) {
                        j += 5;
                        len += 2;
                    } else if (tmpst0 > 11) {
                        j += 6;
                        len += 2;
                    }
                }
            } else {
                j += 1;
                len += 1;
            }
            if (j > bytes.length - 1) {
                break;
            }
        }
        return len;
    }
}
