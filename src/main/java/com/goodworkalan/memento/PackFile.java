package com.goodworkalan.memento;

import java.io.File;

import com.goodworkalan.pack.Pack;

/**
 * A container that maps a pack file to a pack instance.
 *
 * @author Alan Gutierrez
 */
public class PackFile
{
    /** The pack. */
    private final Pack pack;
    
    /** The pack file. */
    private final File file;

    /**
     * Create a mapping between the given file and the given pack.
     * 
     * @param file
     *            The pack file.
     * @param pack
     *            The pack.
     */
    public PackFile(File file, Pack pack)
    {
        this.file = file;
        this.pack = pack;
    }
    
    /**
     * Get the pack file.
     * 
     * @return The pack file.
     */
    public File getFile()
    {
        return file;
    }
    
    /**
     * Get the pack.
     * 
     * @return The pack.
     */
    public Pack getPack()
    {
        return pack;
    }
}
