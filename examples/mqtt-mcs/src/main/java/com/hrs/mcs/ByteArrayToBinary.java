package com.hrs.mcs;

public class ByteArrayToBinary {

    public static String byteToBinary(byte b) {
        StringBuilder binary = new StringBuilder();
        int mask = 0x80; // 0x80 is 10000000 in binary

        for (int i = 0; i < 8; i++) {
            int bit = (b & mask) == 0 ? 0 : 1;
            binary.append(bit);
            mask >>>= 1; // Shift the mask one bit to the right
        }

        return binary.toString();
    }

    public static String byteArrayToBinary(byte[] byteArray) {
        StringBuilder binary = new StringBuilder();
        for (byte b : byteArray) {
            binary.append(byteToBinary(b)).append(" "); // Separate each byte with a space
        }
        return binary.toString().trim();
    }

    public static void main(String[] args) {
        byte[] byteArray = { 0x4A, (byte) 0xAF, (byte) 0xC3, 0x2E };
        String binaryString = byteArrayToBinary(byteArray);
        System.out.println(binaryString);
    }
}

