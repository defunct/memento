package com.goodworkalan.memento;

import java.io.File;

import com.goodworkalan.pack.Pack;

class StrataPointer
{
    private final File file;

    private final Pack pack;
    
    private final long rootAddress;
    
    public StrataPointer(File file, Pack pack, long rootAddress)
    {
        this.file = file;
        this.pack = pack;
        this.rootAddress = rootAddress;
    }
    
    public File getFile()
    {
        return file;
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
