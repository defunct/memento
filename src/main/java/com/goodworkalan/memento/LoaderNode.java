package com.goodworkalan.memento;

/**
 * A node in a linked list of storage definitions stored in individual blocks in
 * a pack file.
 * 
 * @author Alan Gutierrez
 */
public class LoaderNode
{
    /**
     * The address of the next definition in a linked list of definitions in a
     * pack file.
     */
    private long next;
    
    /**
     * The strategy to restore a bin, index or join definition from storage in a
     * block in a pack file.
     */
    private Loader loader;

    /**
     * Create a loader node.
     * 
     * @param next
     *            The address of the next definition in a linked list of
     *            definitions in a pack file.
     * @param loader
     *            The strategy to restore a bin, index or join definition from
     *            storage in a block in a pack file.
     */
    public LoaderNode(long next, Loader loader)
    {
        this.next = next;
        this.loader = loader;
    }
    
    /**
     * Get the address of the next definition in a linked list of definitions in
     * a pack file.
     * 
     * @return The address of the next definition or zero if this is the end of
     *         the list.
     */
    public long getNext()
    {
        return next;
    }

    /**
     * Get the strategy to restore a bin, index or join definition from storage
     * in a block in a pack file.
     * 
     * @return The strategy to restore a bin, index or join definition from
     *         storage in a block in a pack file.
     */
    public Loader getLoader()
    {
        return loader;
    }
}
