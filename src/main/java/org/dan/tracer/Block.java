package org.dan.tracer;

import java.nio.ByteBuffer;

public class Block {
    public static final Block EXIT = new Block(-1, null);

    private final int number;
    private final ByteBuffer buffer;

    public Block(int number, ByteBuffer buffer) {
        this.number = number;
        this.buffer = buffer;
    }

    public int getNumber() {
        return number;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }
}
