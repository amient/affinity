/**
 * Copyright (C) 2015 Michal Harish
 * <p/>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p/>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.amient.affinity.core.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;

public class ByteUtils {

    public static byte[] bufToArray(ByteBuffer b) {
        if (b.hasArray()) {
            if (b.position() == 0 && b.arrayOffset() == 0 && b.limit() == b.capacity()) {
                return b.array();
            } else {
                return Arrays.copyOfRange(b.array(), b.arrayOffset(), b.arrayOffset() + b.remaining());
            }
        } else {
            byte[] a = new byte[b.remaining()];
            int bp = b.position();
            b.get(a);
            b.position(bp);
            return a;
        }
    }

    public static byte[] reverse(byte[] src) {
        byte[] dest = new byte[src.length];
        reverse(src, dest, 0);
        return dest;
    }

    public static void reverse(byte[] src, byte[] dest, int destOffset) {
        int i = destOffset;
        int j = Math.min(dest.length - 1, src.length - 1);
        while (j >= 0 && i < dest.length) {
            dest[i] = src[j];
            j--;
            i++;
        }
    }

    public static int parseIntRadix10(byte[] array, int offset, int limit) {
        int result = 0;
        boolean negative = false;
        if (array[offset] == '-') {
            negative = true;
            offset++;
        }
        for (int i = offset; i <= limit; i++) {
            if (array[i] < 48 || array[i] > 57) {
                throw new IllegalArgumentException("Invalid numeric character " + (char) array[i]);
            }
            result *= 10;
            result += (array[i] - 48);
        }
        return negative ? -result : result;
    }

    public static long parseLongRadix10(byte[] array, int offset, int limit) {
        long result = 0;
        for (int i = offset; i <= limit; i++) {
            if (array[i] < 48 || array[i] > 57) {
                throw new IllegalArgumentException("Invalid numeric character " + (char) array[i]);
            }
            result *= 10;
            result += (array[i] - 48);
        }
        return result;
    }

    public static long parseLongRadix16(byte[] array, int offset, int limit) {
        long result = 0;
        for (int i = offset; i <= limit; i++) {
            result *= 16;
            result += parseRadix16Byte(array[i]);
        }
        return result;
    }

    public static byte[] parseRadix16(byte[] array, int offset, int len) {
        byte[] result = new byte[len / 2];
        parseRadix16(array, offset, len, result, 0);
        return result;
    }

    public static void parseRadix16(byte[] array, int offset, int len, byte[] dest, int destOffset) {
        int limit = Math.min(offset + len, array.length);
        int j = destOffset;
        for (int i = offset; i < limit; i += 2, j++) {
            dest[j] = (byte) (parseRadix16Byte(array[i]) * 16 + parseRadix16Byte(array[i + 1]));
        }
    }

    private static final String HEX = "0123456789abcdef";

    public static String toRadix16(byte[] source, int srcOffset, int len) {
        char[] hex = new char[len * 2];
        for (int j = 0; j < len; j++) {
            int b = source[j + srcOffset] & 0xFF;
            hex[j * 2] = HEX.charAt(b >>> 4);
            hex[j * 2 + 1] = HEX.charAt(b & 0x0F);
        }
        return new String(hex);
    }

    private static int parseRadix16Byte(byte b) {
        if (b >= 48 && b <= 57) {
            return (b - 48);
        } else if (b >= 'A' && b <= 'F') {
            return (b - 55);
        } else if (b >= 97 && b <= 102) {
            return (b - 87);
        } else {
            throw new IllegalArgumentException("Invalid hexadecimal character " + (char) b);
        }
    }

    public static int copy(byte[] src, int srcOffset, byte[] dest, int destOffset, int len) {
        int srcLimit = srcOffset + len;
        for (int i = srcOffset, j = destOffset; i < srcLimit; i++, j++) {
            dest[j] = src[i];
        }
        return len;
    }

    static public int asIntValue(byte[] value) {
        return asIntValue(value, 0);
    }

    static public long asLongValue(byte[] value) {
        return asLongValue(value, 0);
    }

    static public int asIntValue(byte[] value, int offset) {
        return ((((int) value[offset + 0]) << 24) + (((int) value[offset + 1] & 0xff) << 16) + (((int) value[offset + 2] & 0xff) << 8) + (((int) value[offset + 3] & 0xff) << 0));

    }

    static public long asLongValue(byte[] value, int o) {
        return (((long) value[o + 0] << 56) + (((long) value[o + 1] & 0xff) << 48) + (((long) value[o + 2] & 0xff) << 40)
                + (((long) value[o + 3] & 0xff) << 32) + (((long) value[o + 4] & 0xff) << 24) + (((long) value[o + 5] & 0xff) << 16)
                + (((long) value[o + 6] & 0xff) << 8) + (((long) value[o + 7] & 0xff) << 0));
    }

    public static byte[] putIntValue(int value, byte[] result, int offset) {
        result[offset + 0] = (byte) ((value >>> 24) & 0xFF);
        result[offset + 1] = (byte) ((value >>> 16) & 0xFF);
        result[offset + 2] = (byte) ((value >>> 8) & 0xFF);
        result[offset + 3] = (byte) ((value >>> 0) & 0xFF);
        return result;
    }

    public static byte[] putLongValue(long value, byte[] result, int offset) {
        result[offset + 0] = (byte) ((value >>> 56) & 0xFF);
        result[offset + 1] = (byte) ((value >>> 48) & 0xFF);
        result[offset + 2] = (byte) ((value >>> 40) & 0xFF);
        result[offset + 3] = (byte) ((value >>> 32) & 0xFF);
        result[offset + 4] = (byte) ((value >>> 24) & 0xFF);
        result[offset + 5] = (byte) ((value >>> 16) & 0xFF);
        result[offset + 6] = (byte) ((value >>> 8) & 0xFF);
        result[offset + 7] = (byte) ((value >>> 0) & 0xFF);
        return result;
    }

    public static boolean equals(byte[] a, byte[] a2) {
        if (a == a2)
            return true;
        if (a == null || a2 == null)
            return false;

        int length = a.length;
        if (a2.length != length)
            return false;

        for (int i = 0; i < length; i++)
            if (a[i] != a2[i])
                return false;

        return true;
    }

    final public static byte[] max(byte[] lArray, byte[] rArray) {
        int cmp = compare(lArray, 0, lArray.length, rArray, 0, rArray.length);
        if (cmp >= 0) {
            return lArray;
        } else {
            return rArray;
        }
    }

    /**
     * Compares the current buffer position if treated as given type with the
     * given value but does not advance
     */
    final public static boolean equals(byte[] lArray, int leftOffset, int lSize, byte[] rArray, int rightOffset, int rSize) {
        if (lSize != rSize) {
            return false;
        } else {
            return compare(lArray, leftOffset, lSize, rArray, rightOffset, rSize) == 0;
        }
    }

    final public static int compare(byte[] lArray, int leftOffset, int lSize, byte[] rArray, int rightOffset, int rSize) {
        int i = leftOffset;
        int j = rightOffset;
        int n = lSize;
        for (int k = 0; k < n; k++, i++, j++) {
            if (k >= rSize) {
                return 1;
            }
            int cmp = (lArray[i] & 0xFF) - (rArray[j] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        if (lSize < rSize) {
            return -1;
        }
        return 0;
    }

    /**
     * Checks if the current buffer position if treated as given type would
     * contain the given value but does not advance
     */
    final public static boolean contains(byte[] cArray, int cOffset, int cSize, byte[] vArray, int vOffset, int vSize) {
        if (cSize == 0 || vSize == 0 || vSize > cSize) {
            return false;
        }
        int cLimit = cOffset + cSize - 1;
        int vLimit = vOffset + vSize - 1;
        int v = vOffset;
        for (int c = cOffset; c <= cLimit; c++) {
            if (vArray[v] != cArray[c]) {
                v = vOffset;
                if (c + vSize > cLimit) {
                    return false;
                }
            } else if (++v >= vLimit) {
                return true;
            }
        }
        return false;
    }

    public static int crc32(byte[] array, int offset, int size) {
        int crc = 0xFFFF;
        for (int pos = offset; pos < offset + size; pos++) {
            crc ^= (int) array[pos];
            for (int i = 8; i != 0; i--) {
                if ((crc & 0x0001) != 0) {
                    crc >>= 1;
                    crc ^= 0xA001;
                } else {
                    crc >>= 1;
                }
            }
        }
        return crc;
    }

    public static int sum(byte[] array, int offset, int size) {
        int sum = 0;
        for (int i = offset; i < offset + size; i++)
            sum ^= array[i];
        return sum;
    }

    public static byte[] uuid(UUID uuid) {
        byte[] result = new byte[16];
        putLongValue(uuid.getMostSignificantBits(), result, 0);
        putLongValue(uuid.getLeastSignificantBits(), result, 8);
        return result;
    }

    public static UUID uuid(byte[] uuid) {
        return new UUID(asLongValue(uuid, 0), asLongValue(uuid, 8));
    }

    public static byte[] parseUUID(String uuid) {
        return parseUUID(uuid, new byte[16], 0);
    }

    public static byte[] parseUUID(String uuid, byte[] dest, int destOffset) {
        parseUUID(uuid.getBytes(), 1, 0, dest, destOffset);
        return dest;
    }

    public static byte[] parseUUIDNoSpacing(String uuid) {
        byte[] result = new byte[16];
        parseUUID(uuid.getBytes(), 0, 0, result, 0);
        return result;
    }

    public static byte[] parseUUIDNoSpacing(String uuid, byte[] dest, int destOffset) {
        parseUUID(uuid.getBytes(), 0, 0, dest, destOffset);
        return dest;
    }

    public static void parseUUID(byte[] uuid, int spacing, int uuidOffset, byte[] dest, int destOffset) {
        long mostSigBits = parseLongRadix16(uuid, uuidOffset, uuidOffset + 7);
        mostSigBits <<= 16;
        mostSigBits |= parseLongRadix16(uuid, uuidOffset + 8 + 1 * spacing, uuidOffset + 11 + 1 * spacing);
        mostSigBits <<= 16;
        mostSigBits |= parseLongRadix16(uuid, uuidOffset + 12 + 2 * spacing, uuidOffset + 15 + 2 * spacing);
        long leastSigBits = parseLongRadix16(uuid, uuidOffset + 16 + 3 * spacing, uuidOffset + 19 + 3 * spacing);
        leastSigBits <<= 48;
        leastSigBits |= parseLongRadix16(uuid, uuidOffset + 20 + 4 * spacing, uuidOffset + 31 + 4 * spacing);
        putLongValue(mostSigBits, dest, destOffset);
        putLongValue(leastSigBits, dest, destOffset + 8);
    }

    public static String UUIDToString(byte[] src, int offset) {
        return UUIDToString(asLongValue(src, offset), asLongValue(src, offset + 8), "-");
    }

    public static String UUIDToNumericString(byte[] src, int offset) {
        return UUIDToString(asLongValue(src, offset), asLongValue(src, offset + 8), "");
    }

    private static String UUIDToString(long mostSigBits, long leastSigBits, String separator) {
        return (digits(mostSigBits >> 32, 8)
                + separator + digits(mostSigBits >> 16, 4)
                + separator + digits(mostSigBits, 4)
                + separator + digits(leastSigBits >> 48, 4)
                + separator + digits(leastSigBits, 12));
    }

    private static String digits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }
}
