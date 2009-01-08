package com.goodworkalan.memento;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public final class ByteBufferInputStream
extends InputStream
{
    private final ByteBuffer bytes;

    public ByteBufferInputStream(ByteBuffer bytes)
    {
        this.bytes = bytes;
    }

    public int read() throws IOException
    {
        if (!bytes.hasRemaining())
        {
            return -1;
        }
        return bytes.get() & 0xff;
    }

    public int read(byte[] b, int off, int len) throws IOException
    {
        len = Math.min(len, bytes.remaining());
        if (len == 0)
        {
            return -1;
        }
        bytes.get(b, off, len);
        return len;
    }
}