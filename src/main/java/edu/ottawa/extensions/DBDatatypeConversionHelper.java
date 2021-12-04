package edu.ottawa.extensions;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Conversion Helper class to convert data types supported into byte for storage
 * and vice versa for display and operations
 *
 * @author Team Blue
 */
public class DBDatatypeConversionHelper {

    /**
     * Helper that return value as byte array
     *
     * @param data
     * @return
     */
    public static Byte[] byteToBytes(final byte[] data) {
        int length = data == null ? 0 : data.length;
        Byte[] result = new Byte[length];
        for (int i = 0; i < length; i++)
            result[i] = data[i];
        return result;
    }

    /**
     * Unboxing wrapper helper
     *
     * @param data
     * @return
     */
    public static byte[] Bytestobytes(final Byte[] data) {

        if (data == null) System.out.println("! Data is null");

        int length = data == null ? 0 : data.length;
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++)
            result[i] = data[i];
        return result;
    }

    /* Function that return bytes from primitive data types*/
    public static Byte[] shortToBytes(final short data) {
        return byteToBytes(ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(data).array());
    }

    public static byte[] shortTobytes(final short data) {
        return ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(data).array();
    }

    public static Byte[] intToBytes(final int data) {
        return byteToBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(data).array());
    }

    public static byte[] intTobytes(final int data) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(data).array();
    }

    public static byte[] longTobytes(final long data) {
        return ByteBuffer.allocate(Long.BYTES).putLong(data).array();
    }

    public static byte[] floatTobytes(final float data) {
        return (ByteBuffer.allocate(Float.BYTES).putFloat(data).array());
    }

    public static byte[] doubleTobytes(final double data) {
        return (ByteBuffer.allocate(Double.BYTES).putDouble(data).array());
    }

    /* Function to get primitives from byte values*/
    public static byte byteFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).get();
    }

    public static short shortFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getShort();
    }

    public static int intFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static long longFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    public static float floatFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getFloat();
    }

    public static double doubleFromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }
}