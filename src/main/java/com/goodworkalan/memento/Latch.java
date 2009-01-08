package com.goodworkalan.memento;



public final class Latch
implements Sync
{
    private boolean unlatched;
    
    public synchronized void acquire()
    {
        while (!unlatched)
        {
            try
            {
                wait();
            }
            catch (InterruptedException e)
            {
            }
        }
    }
    
    public void release()
    {
        if (!unlatched)
        {
            unlatched = true;
            notifyAll();
        }
    }
}