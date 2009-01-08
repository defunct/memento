package com.goodworkalan.memento;


public class JoinAdvancer
{
    private final Strata.Cursor cursor;

    private JoinRecord record;

    private boolean atEnd;

    public JoinAdvancer(Strata.Cursor cursor)
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
        record = (com.goodworkalan.memento.Record) cursor.next();
        return true;
    }

    public void release()
    {
        cursor.release();
    }
}
