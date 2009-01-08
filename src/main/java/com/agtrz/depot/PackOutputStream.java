package com.agtrz.depot;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import com.goodworkalan.pack.Mutator;

final class PackOutputStream
extends ByteArrayOutputStream
{
    private final Mutator mutator;

    public PackOutputStream(Mutator mutator)
    {
        this.mutator = mutator;
    }

    private void write(long address)
    {
        ByteBuffer bytes = ByteBuffer.allocateDirect(size());
        bytes.put(toByteArray());
        bytes.flip();

        mutator.write(address, bytes);
    }

    public long allocate()
    {
        long address = mutator.allocate(size());
        write(address);
        return address;
    }

    public long temporary()
    {
        long address = mutator.temporary(size());
        write(address);
        return address;
    }
}