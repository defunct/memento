package com.goodworkalan.memento;

import com.goodworkalan.strata.Cursor;


public class JoinAdvancer
{
    private final Cursor<JoinRecord> cursor;

    private JoinRecord record;

    private boolean atEnd;

    public JoinAdvancer(Cursor<JoinRecord> cursor)
    {
        this.cursor = cursor;
    }

    public JoinRecord getRecord()
    {
        return record;
    }

    public boolean getAtEnd()
    {
        return atEnd;
    }

    public boolean advance()
    {
        if (!cursor.hasNext())
        {
            atEnd = true;
            cursor.release();
            return false;
        }
        record = cursor.next();
        return true;
    }

    public void release()
    {
        cursor.release();
    }
}
