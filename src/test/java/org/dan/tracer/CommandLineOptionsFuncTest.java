package org.dan.tracer;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class CommandLineOptionsFuncTest {
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
            ByteBuffer buf = ByteBuffer.allocate(5);
            options.getInputCh().read(buf);
            buf.flip();
            assertEquals("hello", new String(buf.array()));
        } finally {
            Files.deleteIfExists(input);
        }
    }
}
