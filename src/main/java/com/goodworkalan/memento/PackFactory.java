package com.goodworkalan.memento;

import java.io.File;

import com.goodworkalan.ilk.Ilk;

// TODO Document.
public interface PackFactory 
{
    /**
     * Create a new instance of a memento database.
     */
    public void create();

    /**
     * Open the memento database. Return true if the database was shutdown soft
     * and is ready to load. Return false if the database was shutdown hard.
     * 
     * @return True if the database was shutdown soft, false if the database was
     *         shutdown soft.
     */
    public boolean open();
    
    /**
     * Recover a database that was shutdown hard.
     */
    public void recover();
    
    /** Load a successfully opened database. */
    public void load();
    
    /** Softly shutdown the database. */
    public void close();

    /**
     * Get the snapshot index.
     * 
     * @return A structure containing a b-tree and the pack file that contains
     *         it.
     */
    public PackStrata<SnapshotRecord> getSnapshots();

    /**
     * Create storage for a bin. This will allocate heap storage for objects and
     * an index to find the objects by a key identifieF
     * 
     * @param bin
     *            The super type token of the bin.
     */
    public void create(Ilk.Key bin);

    /**
     * Get the bin index for a given super type token.
     * 
     * @param bin
     *            The super type token of the objects contained in the bin.
     * @return A structure containing a b-tree and the pack file that contains
     *         it.
     */
    public PackStrata<BinRecord> getBinIndex(Ilk.Key bin);

    /**
     * Get the pack file used to allocate blocks to store objects for the bin of
     * the given super type token.
     * 
     * @param bin
     *            The super type token of the objects contained in the bin.
     * @return A the pack file used to store objects for the bin.
     */
    public PackFile getBinHeap(Ilk.Key bin);

    /**
     * Add a bin read from a definition to the storage.
     * 
     * @param address
     *            The address where this definition is stored.
     * @param key
     *            The super type token of the object stored in the bin.
     * @param heapFile
     *            The pack file containing the bin heap.
     * @param indexFile
     *            The pack file containing the bin index.
     * @param indexAddress
     *            The address of the index in fine containing the bin index.
     */
    public void load(long address, Ilk.Key key, File heapFile, File indexFile, long indexAddress);

    public void remove(Ilk.Key bin);

    public void create(Ilk.Key bin, Ilk.Key index);
    
    public PackStrata<IndexRecord> getIndex(Ilk.Key bin, Ilk.Key index);
    
    public void remove(Ilk.Key bin, Ilk.Key index);

    public void create(Link link);
    
    public PackStrata<JoinRecord> getJoin(Link link);
    
    public void remove(Link link);
}
