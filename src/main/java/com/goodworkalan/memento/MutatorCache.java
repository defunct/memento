package com.goodworkalan.memento;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.pack.Mutator;

// TODO Document.
public class MutatorCache
{
    /** A map of file names to pack mutators. */
    private final Map<File, Mutator> mutators = new HashMap<File, Mutator>();

    /**
     * Obtain a mutator for the given pack and pack file.
     * 
     * @param packFile
     *            A pack to file mapping.
     * @return A mutator for the given pack and pack file.
     */
    public Mutator mutate(PackFile packFile)
    {
        Mutator mutator = mutators.get(packFile.getFile());
        if (mutator == null)
        {
            mutator = packFile.getPack().mutate();
            mutators.put(packFile.getFile(), mutator);
        }
        return mutator;
    }
    
    /**
     * Rollback all the mutators held by this mutator cache.
     */
    public void rollback()
    {
        for (Mutator mutator : mutators.values())
        {
            mutator.rollback();
        }
    }
    
    /**
     * Commit all the pack mutators held by this mutator cache.
     */
    public void commit()
    {
        for (Mutator mutator : mutators.values())
        {
            mutator.commit();
        }
    }
}
