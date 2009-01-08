package com.agtrz.depot;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.goodworkalan.fossil.RecordIO;

public class SnapshotRecordIO implements RecordIO<SnapshotRecord>, Serializable
{
    private static final long serialVersionUID = 20090107L;

    public int getSize()
    {
        // TODO Auto-generated method stub
        return 0;
    }
    
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
