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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public class ByteUtils {


    /**
     * Converts ByteBuffer to an array - if the buffer is backed by the array but doesn't
     * fully overlap it it performs an array copy. If the buffer is not backed by an array
     * it constructs a new array and reads the buffer content into it.
     * @param b byte buffer to be converted to an array
     * @return all remaining bytes from the bytebuffer as a byte array
     */
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

    /**
     * Reverse byte array contents
     * @param src array to be reversed
     * @return reversed byte array
     */
    public static byte[] reverse(byte[] src) {
        byte[] dest = new byte[src.length];
        reverse(src, dest, 0);
        return dest;
    }

    /**
     * Reverse byte array contents
     * @param src array to be reversed
     * @param dest destination array
     * @param destOffset position in the destination array
     */
    public static void reverse(byte[] src, byte[] dest, int destOffset) {
        int i = destOffset;
        int j = Math.min(dest.length - 1, src.length - 1);
        while (j >= 0 && i < dest.length) {
            dest[i] = src[j];
            j--;
            i++;
        }
    }

    /**
     * Parse an array of character digits with base of 10
     * @param array containing the digits to be parsed
     * @param offset position where to start the parsing
     * @param limit position of the last digit to parse
     * @return int equivalent of the parsed number
     */
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


    /**
     * Parse an array of character digits with base of 10
     * @param array containing the digits to be parsed
     * @param offset position where to start the parsing
     * @param limit position of the last digit to parse
     * @return long equivalent of the parsed number
     */
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

    /**
     * Parse an array of hexadecimal characters as a long value
     * @param array containing the digits to be parsed
     * @param offset position where to start the parsing
     * @param limit position of the last digit to parse
     * @return a long value represented by the hexadecimal input
     */
    public static long parseLongRadix16(byte[] array, int offset, int limit) {
        long result = 0;
        for (int i = offset; i <= limit; i++) {
            result *= 16;
            result += parseRadix16Byte(array[i]);
        }
        return result;
    }

    /**
     * Parse an array of hexadecimal characters
     * @param array containing the hexadecimal characters
     * @param offset position where to start the parsing
     * @param len number of digits to parse
     * @return byte array as an n-bit representation of the input hexadecimal number
     */
    public static byte[] parseRadix16(byte[] array, int offset, int len) {
        byte[] result = new byte[len / 2];
        parseRadix16(array, offset, len, result, 0);
        return result;
    }

    /**
     * Parse an array of hexadecimal characters
     * @param array containing the hexadecimal characters
     * @param offset position where to start the parsing
     * @param len number of digits to parse
     * @param dest array where the n-bit representation result will be stored
     * @param destOffset position in the dest array where the result will be written
     */
    public static void parseRadix16(byte[] array, int offset, int len, byte[] dest, int destOffset) {
        int limit = Math.min(offset + len, array.length);
        int j = destOffset;
        for (int i = offset; i < limit; i += 2, j++) {
            dest[j] = (byte) (parseRadix16Byte(array[i]) * 16 + parseRadix16Byte(array[i + 1]));
        }
    }

    private static final String HEX = "0123456789abcdef";

    /**
     * Convert an n-bit BIG-ENDIAN representation of a number into its hexadecimal form
     * @param source an array containing the n
     * @param srcOffset position in the source array where to start
     * @param len number of bytes to convert
     * @return string of hexadecimal digits
     */
    public static String toRadix16(byte[] source, int srcOffset, int len) {
        char[] hex = new char[len * 2];
        for (int j = 0; j < len; j++) {
            int b = source[j + srcOffset] & 0xFF;
            hex[j * 2] = HEX.charAt(b >>> 4);
            hex[j * 2 + 1] = HEX.charAt(b & 0x0F);
        }
        return new String(hex);
    }

    /**
     * Parse a digit character with a base of 16 into its integer equivalent
     * @param b byte representing the ordinal digit, e.g. one of 0123456789ABCDEFabcdef
     * @return int from 0 to 16
     */
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

    /**
     * Copy a range of an array into a dest array
     * @param src source array
     * @param srcOffset source array start position
     * @param dest destination array
     * @param destOffset destination array start position
     * @param len maximum number of bytes to copy
     * @return number of bytes copied
     */
    public static int copy(byte[] src, int srcOffset, byte[] dest, int destOffset, int len) {
        int srcLimit = srcOffset + len;
        for (int i = srcOffset, j = destOffset; i < srcLimit; i++, j++) {
            dest[j] = src[i];
        }
        return len;
    }

    /**
     * Convert 32-bit BIG ENDIAN array into an integer
     * @param value array containing at least 4 bytes
     * @return int
     */
    static public int asIntValue(byte[] value) {
        return asIntValue(value, 0);
    }

    /**
     * Convert 64-bit BIG ENDIAN array into a long
     * @param value array containing at least 8 bytes
     * @return long
     */
    static public long asLongValue(byte[] value) {
        return asLongValue(value, 0);
    }

    /**
     * Convert 32-bit BIG ENDIAN array into an integer
     * @param value array containing at least 4 bytes
     * @param offset position of the first byte in the value array
     * @return int
     */
    static public int asIntValue(byte[] value, int offset) {
        return ((((int) value[offset + 0]) << 24)
                + (((int) value[offset + 1] & 0xff) << 16)
                + (((int) value[offset + 2] & 0xff) << 8)
                + (((int) value[offset + 3] & 0xff) << 0));

    }

    /**
     * Read 32-bit BIG ENDIAN number from an input stream
     * @param in inpust stream
     * @return int
     * @throws IOException if the inpust stream cannot be read
     */
    static public int readIntValue(InputStream in) throws IOException {
        return (in.read() << 24) + ((in.read() & 0xff) << 16) + ((in.read() & 0xff) << 8) + (in.read() & 0xff);
    }

    /**
     * Convert 64-bit BIG ENDIAN array into a long
     * @param value array containing at least 8 bytes
     * @param o offset position of the first byte in the value array
     * @return long
     */

    static public long asLongValue(byte[] value, int o) {
        return (((long) value[o + 0] << 56) + (((long) value[o + 1] & 0xff) << 48) + (((long) value[o + 2] & 0xff) << 40)
                + (((long) value[o + 3] & 0xff) << 32) + (((long) value[o + 4] & 0xff) << 24) + (((long) value[o + 5] & 0xff) << 16)
                + (((long) value[o + 6] & 0xff) << 8) + (((long) value[o + 7] & 0xff) << 0));
    }

    /**
     * Generate 32-bit BIG ENDIAN number into a byte array
     * @param value int number to convert
     * @return a new 4-byte array with encoded integer
     */
    public static byte[] intValue(int value) {
        byte[] result = new byte[4];
        return putIntValue(value, result, 0);
    }

    /**
     * Write 32-bit BIG ENDIAN number into a byte array
     * @param value int number to convert
     * @param result destination array where to write the big endian
     * @param offset position in the destination array to start from
     * @return the modified array passed in the result parameterInfo
     */
    public static byte[] putIntValue(int value, byte[] result, int offset) {
        result[offset + 0] = (byte) ((value >>> 24) & 0xFF);
        result[offset + 1] = (byte) ((value >>> 16) & 0xFF);
        result[offset + 2] = (byte) ((value >>> 8) & 0xFF);
        result[offset + 3] = (byte) ((value >>> 0) & 0xFF);
        return result;
    }

    /**
     * Write 32-bit BIG ENDIAN number into an output stream
     * @param value int number to write
     * @param out destination output stream where to write the big endian
     * @throws IOException if the output stream cannot be written to
     */
    public static void writeIntValue(int value, OutputStream out) throws IOException {
        out.write((value >>> 24) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write((value >>> 0) & 0xFF);
    }

    /**
     * Write 64-bit BIG ENDIAN number into an output stream
     * @param value long number to write
     * @param out destination output stream where to write the big endian
     * @throws IOException if the output stream cannot be written to
     */
    public static void writeLongValue(long value, OutputStream out) throws IOException {
        out.write((byte)((value >>> 56) & 0xFF));
        out.write((byte)((value >>> 48) & 0xFF));
        out.write((byte)((value >>> 40) & 0xFF));
        out.write((byte)((value >>> 32) & 0xFF));
        out.write((byte)((value >>> 24) & 0xFF));
        out.write((byte)((value >>> 16) & 0xFF));
        out.write((byte)((value >>> 8) & 0xFF));
        out.write((byte)((value >>> 0) & 0xFF));
    }

    /**
     * Generate 64-bit BIG ENDIAN number into a byte array
     * @param value long number to convert
     * @return a new 8-byte array with encoded integer
     */
    public static byte[] longValue(long value) {
        byte[] result = new byte[8];
        return putLongValue(value, result, 0);
    }


    /**
     * Write 64-bit BIG ENDIAN number into a byte array
     * @param value long number to convert
     * @param result destination array where to write the big endian
     * @param offset position in the destination array to start from
     * @return the modified array passed in the result parameterInfo
     */
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

    /**
     * Equality check for 2 byte arrays
     * @param a first array
     * @param a2 second array
     * @return true if the arrays are identical, false otherwise
     */
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

    /**
     * BIG ENDIAN comparison of 2 byte arrays
     * @param lArray left array
     * @param rArray right array
     * @return the larger of the 2 arguments
     */
    final public static byte[] max(byte[] lArray, byte[] rArray) {
        int cmp = compare(lArray, 0, lArray.length, rArray, 0, rArray.length);
        if (cmp >= 0) {
            return lArray;
        } else {
            return rArray;
        }
    }

    /**
     * BIG ENDIAN comparison of 2 ranges of byte arrays
     * @param lArray left array
     * @param leftOffset left array range start
     * @param lSize left array range size
     * @param rArray right array
     * @param rightOffset right array range start
     * @param rSize right array range size
     * @return true if the ranges are identical, false otherwise
     */
    final public static boolean equals(byte[] lArray, int leftOffset, int lSize, byte[] rArray, int rightOffset, int rSize) {
        if (lSize != rSize) {
            return false;
        } else {
            return compare(lArray, leftOffset, lSize, rArray, rightOffset, rSize) == 0;
        }
    }

    /**
     * BIG ENDIAN comparison of 2 ranges of byte arrays. If the 2 arrays have different size
     * they are still compared. If for example the left array has a smaller size than the right array
     * but all the bytes are identical to the head of the right array, the result is -1.
     * @param lArray left array
     * @param leftOffset left array range start
     * @param lSize left array range size
     * @param rArray right array
     * @param rightOffset right array range start
     * @param rSize right array range size
     * @return -1 if left array is smaller, 1 if the left array is larger, 0 if the 2 ranges are identical
     */
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
     * Check if one byte array range is contained in another
     * @param cArray the content array which is searched through
     * @param cOffset the content array range where the search starts
     * @param cSize the content array range num bytes
     * @param vArray the value array that is searched for
     * @param vOffset the value array range start
     * @param vSize the value array range num bytes
     * @return true if the cArray contains the vArray
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

    /**
     * Check if one byte array range starts with another
     * @param cArray the content array which is searched through
     * @param vArray the value array that is searched for
     * @return true if the cArray starts with the vArray
     */
    final public static boolean startsWith(byte[] cArray, byte[] vArray) {
        if (cArray.length < vArray.length) return false;
        int cLimit = vArray.length - 1;
        for (int c = 0; c <= cLimit; c++) {
            if (vArray[c] != cArray[c]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if one byte buffer starts with another
     * @param cBuf the content buffer which is searched through
     * @param vBuf the value buffer that is searched for
     * @return true if the cBuf starts with the vBug
     */
    final public static boolean startsWith(ByteBuffer cBuf, ByteBuffer vBuf) {
        if (cBuf.limit() < vBuf.limit()) return false;
        int cLimit = vBuf.limit() - 1;
        for (int c = 0; c <= cLimit; c++) {
            if (vBuf.get(vBuf.position()+c) != cBuf.get(cBuf.position()+c)) {
                return false;
            }
        }
        return true;
    }

    /**
     * CRC32 checksum of a byte array range
     * @param array input array to perform the checksum over
     * @param offset start position
     * @param size number of bytes to checksum from the offset
     * @return int checksum
     */
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

    /**
     * XOR checksum of a byte array range
     * @param array input array to checksum
     * @param offset start position in the input
     * @param size number of bytes to checksum
     * @return int checksum
     */
    public static int sum(byte[] array, int offset, int size) {
        int sum = 0;
        for (int i = offset; i < offset + size; i++)
            sum ^= array[i];
        return sum;
    }

    /**
     * convert java.util.UUID into 128-bit representation as a byte array
     * @param uuid input java.util.UUID
     * @return byte array
     */
    public static byte[] uuid(UUID uuid) {
        byte[] result = new byte[16];
        putLongValue(uuid.getMostSignificantBits(), result, 0);
        putLongValue(uuid.getLeastSignificantBits(), result, 8);
        return result;
    }

    /**
     * convert 128-bit representation into a java.util.UUID
     * @param uuid input byte array
     * @return java.util.UUID
     */
    public static UUID uuid(byte[] uuid) {
        return new UUID(asLongValue(uuid, 0), asLongValue(uuid, 8));
    }

    /**
     * parse a standard string representation of a UUID (hexadecimal components spaced with dash)
     * @param uuid input string
     * @return 128-bit UUID representation
     */
    public static byte[] parseUUID(String uuid) {
        return parseUUID(uuid, new byte[16], 0);
    }

    /**
     * convert a standard string representation of a UUID (hexadecimal components spaced with dash)
     * from a byte string range into a 128-bit UUID representation
     * @param uuid input byte string
     * @param dest output byte array
     * @param destOffset output byte array start position
     * @return modified dest array
     */
    public static byte[] parseUUID(String uuid, byte[] dest, int destOffset) {
        parseUUID(uuid.getBytes(), 1, 0, dest, destOffset);
        return dest;
    }

    /**
     * parse a pure hexadecimal string representation of a UUID
     * @param uuid input byte string
     * @return 128-bit UUID representation
     */
    public static byte[] parseUUIDNoSpacing(String uuid) {
        byte[] result = new byte[16];
        parseUUID(uuid.getBytes(), 0, 0, result, 0);
        return result;
    }

    /**
     * parse a pure hexadecimal string representation of a UUID into a specific position in the dest byte array
     * @param uuid input byte string
     * @param dest output byte array
     * @param destOffset output byte array start position
     * @return 128-bit UUID representation
     */
    public static byte[] parseUUIDNoSpacing(String uuid, byte[] dest, int destOffset) {
        parseUUID(uuid.getBytes(), 0, 0, dest, destOffset);
        return dest;
    }

    private static void parseUUID(byte[] uuid, int spacing, int uuidOffset, byte[] dest, int destOffset) {
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

    /**
     * convert 128-bit UUID into its String representation with dashes separating the UUID components
     * @param src byte array containing 16 bytes of the UUID value
     * @param offset position in the src array where the UUID starts
     * @return standard UUID string representation
     */
    public static String UUIDToString(byte[] src, int offset) {
        return UUIDToString(asLongValue(src, offset), asLongValue(src, offset + 8), "-");
    }

    /**
     * convert 128-bit UUID into its String representation without dashes separating the UUID components
     * @param src byte array containing 16 bytes of the UUID value
     * @param offset position in the src array where the UUID starts
     * @return hexadecimal string representation of the UUID
     */
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

    //the murmur2 function is identical to that of kafka (0.9+) and this is also tested under each kafka module
    public static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= (data[length & ~3] & 0xff);
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }


}
