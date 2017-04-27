package org.dan.tracer;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class EndianTest {
    @Test
    public void little() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(0x33445577);
        byte[] result = byteBuffer.array();
        assertEquals(new String(new byte [] { 0x77, 0x55, 0x44, 0x33 }), new String(result));
    }

    @Test
    public void big() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.order(ByteOrder.BIG_ENDIAN);
        byteBuffer.putInt(0x33445577);
        byte[] result = byteBuffer.array();
        assertEquals(new String(new byte [] { 0x33, 0x44, 0x55, 0x77}), new String(result));
    }
}
