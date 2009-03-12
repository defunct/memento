package com.goodworkalan.memento;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.goodworkalan.fossil.RecordIO;

// TODO Document.
public class SnapshotRecordIO implements RecordIO<SnapshotRecord>, Serializable
{
    // TODO Document.
    private static final long serialVersionUID = 20090107L;

    // TODO Document.
    public int getSize()
    {
        return (Long.SIZE + Integer.SIZE) / Byte.SIZE;
    }
    
    // TODO Document.
    public void write(ByteBuffer bytes, SnapshotRecord object)
    {
        if (object == null)
        {
            bytes.putLong(0L);
            bytes.putInt(0);
        }
        else
        {
            bytes.putLong(object.version);
            bytes.putInt(object.state);
        }
    }
    
    // TODO Document.
    public SnapshotRecord read(ByteBuffer bytes)
    {
        long version = bytes.getLong();
        Integer state = bytes.getInt();
        if (version == 0L)
        {
            return null;
        }
        return new SnapshotRecord(version, state);
    }
}
