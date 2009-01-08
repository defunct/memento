package com.agtrz.depot;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.goodworkalan.fossil.RecordIO;

public class BinRecordIO implements RecordIO<BinRecord>, Serializable
{
    private static final long serialVersionUID = 20090107L;

    public int getSize()
    {
        return Long.SIZE / Byte.SIZE * 3;
    }
    
    public BinRecord read(ByteBuffer bytes)
    {
        return new BinRecord(bytes.getLong(), bytes.getLong(), bytes.getLong());
    }
    
    public void write(ByteBuffer bytes, BinRecord record)
    {
        bytes.putLong(record.key);
        bytes.putLong(record.version);
        bytes.putLong(record.address);
    }
}
