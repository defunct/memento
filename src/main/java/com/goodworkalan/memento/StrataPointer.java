package com.goodworkalan.memento;

import com.goodworkalan.pack.Pack;

class StrataPointer
{
    private final Pack pack;
    
    private final long rootAddress;
    
    public StrataPointer(Pack pack, long rootAddress)
    {
        this.pack = pack;
        this.rootAddress = rootAddress;
    }

    public Pack getPack()
    {
        return pack;
    }
    
    public long getRootAddress()
    {
        return rootAddress;
    }
}
