package com.agtrz.depot;

public class JoinAdvancer
{
    private final Strata.Cursor cursor;

    private Join.Record record;

    private boolean atEnd;

    public JoinAdvancer(Strata.Cursor cursor)
    {
        this.cursor = cursor;
    }

    public Join.Record getRecord()
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
        record = (Join.Record) cursor.next();
        return true;
    }

    public void release()
    {
        cursor.release();
    }
}
