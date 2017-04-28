package org.dan.tracer;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class CommandLineOptionsTest {
    private CommandLineOptions options;

    @Before
    public void setUp() {
        options = new CommandLineOptions();
    }

    @Test
    public void overrideInput() throws IOException {
        final Path input = Files.createTempFile("input", "xx");
        try {
            Files.write(input, "hello".getBytes());
            final ReadableByteChannel defaultInput = options.getInputCh();
            options.parse(new String[] {"-in", input.toString()});
            assertFalse(defaultInput == options.getInputCh());
            try {
                ByteBuffer buf = ByteBuffer.allocate(5);
                options.getInputCh().read(buf);
                buf.flip();
                assertEquals("hello", new String(buf.array()));
            } finally {
                options.getInputCh().close();
            }
        } finally {
            Files.deleteIfExists(input);
        }
    }

    @Test
    public void overrideOutput() throws IOException {
        final Path output = Files.createTempFile("output", "xx");
        try {
            final WritableByteChannel defaultOutput = options.getOutputCh();
            options.parse(new String[] {"-out", output.toString()});
            assertFalse(defaultOutput == options.getOutputCh());
            try {
                options.getOutputCh().write(ByteBuffer.wrap("hello".getBytes()));
                assertEquals("hello", new String(Files.readAllBytes(output)));
            } finally {
                options.getOutputCh().close();
            }
        } finally {
            Files.deleteIfExists(output);
        }
    }

    @Test
    public void overrideReadBuffer() throws IOException {
        options.parse(new String[] {"-rbuf", "12345"});
        assertEquals(options.getReadBufferBytes(), 12345);
    }

    @Test
    public void overrideWriteBuffer() throws IOException {
        options.parse(new String[] {"-wbuf", "12345"});
        assertEquals(options.getWriteBufferBytes(), 12345);
    }
}
