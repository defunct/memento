package com.goodworkalan.memento;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.goodworkalan.fossil.RecordIO;

public class IndexRecordIO implements RecordIO<IndexRecord>, Serializable
{
    private static final long serialVersionUID = 20090107L;

    public int getSize()
    {
        return Long.SIZE / Byte.SIZE * 2;
    }
    
    public IndexRecord read(ByteBuffer bytes)
    {
        return new IndexRecord(bytes.getLong(), bytes.getLong());
    }
    
    public void write(ByteBuffer bytes, IndexRecord object)
    {
        bytes.putLong(object.key);
        bytes.putLong(object.version);
    }
}
