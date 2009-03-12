package com.goodworkalan.memento;

/**
 * A strategy to restore a bin, index or join definition from storage in a block
 * in a pack file. Implementations are serialized to a block per definition in a
 * pack with a pointer to the next definition block. They are deserialized from
 * the pack file during load. Each loader is asked to add itself to the pack
 * factory.
 * 
 * @author Alan Gutierrez
 */
public interface Loader
{
    /**
     * Load the definition stored by this object at the given address in the
     * definition pack file into the given pack factory.
     * 
     * @param address
     *            The address where this definition is stored.
     * @param packFactory
     *            The pack factory.
     */
    public void load(long address, PackFactory packFactory);
}
