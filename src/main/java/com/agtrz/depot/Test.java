package com.agtrz.depot;

import com.goodworkalan.memento.Sync;

public final class Test
{
    private Sync registerMutation;

    private Sync changesWritten;

    Sync journalComplete;

    Test(Sync changesWritten, Sync registerMutation, Sync journalComplete)
    {
        this.changesWritten = changesWritten;
        this.registerMutation = registerMutation;
        this.journalComplete = journalComplete;
    }

    void changesWritten()
    {
        changesWritten.release();
        registerMutation.acquire();
    }

    public void waitForChangesWritten()
    {
        changesWritten.acquire();
    }

    public void registerMutation()
    {
        registerMutation.release();
    }

    public void waitForCompletion()
    {
        journalComplete.acquire();
    }
}