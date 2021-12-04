package edu.ottawa.extensions;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

//This is a helper class to convert supported data types supported during storage and display.
public class DBDatatypeConversionHelper {

    //Helper returns value as byte array
    public static Byte[] byteToBytes(final byte[] data) {
        int data_length = data == null ? 0 : data.length;
        Byte[] result = new Byte[data_length];
        for (int i = 0; i < data_length; i++)
            result[i] = data[i];
        return result;
    }

    //Unboxing helper used for wrapping
    public static byte[] Bytestobytes(final Byte[] data) {

        if (data == null) System.out.println("Data is null");

        int data_length = data == null ? 0 : data.length;
        byte[] result = new byte[data_length];
        for (int i = 0; i < data_length; i++)
            result[i] = data[i];
        return result;
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

    //Function that return bytes from primitive data types
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

}