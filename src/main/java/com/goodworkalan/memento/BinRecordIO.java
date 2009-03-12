package com.goodworkalan.memento;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.goodworkalan.fossil.RecordIO;

// TODO Document.
public class BinRecordIO implements RecordIO<BinRecord>, Serializable
{
    // TODO Document.
    private static final long serialVersionUID = 20090107L;

    // TODO Document.
    public int getSize()
    {
        return Long.SIZE / Byte.SIZE * 3;
    }
    
    // TODO Document.
    public BinRecord read(ByteBuffer bytes)
    {
        return new BinRecord(bytes.getLong(), bytes.getLong(), bytes.getLong());
    }
    
    // TODO Document.
    public void write(ByteBuffer bytes, BinRecord record)
    {
        bytes.putLong(record.key);
        bytes.putLong(record.version);
        bytes.putLong(record.address);
    }
}
