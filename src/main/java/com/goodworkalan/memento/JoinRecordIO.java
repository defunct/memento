package com.goodworkalan.memento;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.goodworkalan.fossil.RecordIO;

public class JoinRecordIO implements RecordIO<JoinRecord>, Serializable
{
    private static final long serialVersionUID = 20090107L;

    private final int size;
    
    public JoinRecordIO(int size)
    {
        this.size = size;
    }
    
    public int getSize()
    {
        return (Long.SIZE / Byte.SIZE * (size + 1)) + Short.SIZE / Byte.SIZE;
    }
    
    public void write(ByteBuffer bytes, JoinRecord object)
    {
        long[] keys = object.keys;
        for (int i = 0; i < size; i++)
        {
            bytes.putLong(keys[i]);
        }
        bytes.putLong(object.version);
        bytes.putShort(object.deleted ? (short) 1 : (short) 0);
    }

    public JoinRecord read(ByteBuffer bytes)
    {
        long[] keys = new long[size];
        for (int i = 0; i < size; i++)
        {
            keys[i] = bytes.getLong();
        }
        return new JoinRecord(keys, bytes.getLong(), bytes.getShort() == 1);
    }
}
