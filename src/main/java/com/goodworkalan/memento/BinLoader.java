package com.goodworkalan.memento;

import java.io.File;
import java.io.Serializable;

import com.goodworkalan.ilk.Ilk;

/**
 * A strategy to load a bin definition from storage.
 * 
 * @author Alan Gutierrez
 */
public class BinLoader implements Loader, Serializable
{
    /** Serial version id. */
    private static final long serialVersionUID = 1L;
    
    /** The super type token of the object stored in the bin. */
    private Ilk.Key key;
    
    /** The pack file containing the bin heap. */
    private File heapFile;
    
    /** The pack file containing the bin index. */
    private File indexFile;
    
    /** The address of the index in fine containing the bin index. */
    private long indexAddress;

    /**
     * Create a new bin loader.
     * 
     * @param key
     *            The super type token of the object stored in the bin.
     * @param heapFile
     *            The pack file containing the bin heap.
     * @param indexFile
     *            The pack file containing the bin index.
     * @param indexAddress
     *            The address of the index in fine containing the bin index.
     */
    public BinLoader(Ilk.Key key, File heapFile, File indexFile, long indexAddress)
    {
        this.key = key;
        this.heapFile = heapFile;
        this.indexFile = indexFile;
        this.indexAddress = indexAddress;
    }
    
    /**
     * Load the bin definition stored by this object at the given address in the
     * definition pack file into the given pack factory.
     * 
     * @param address
     *            The address where this definition is stored.
     * @param packFactory
     *            The pack factory.
     */
    public void load(long address, PackFactory packFactory)
    {
        packFactory.load(address, key, heapFile, indexFile, indexAddress);
    }
}
