package com.goodworkalan.memento;

import java.io.File;

import com.goodworkalan.pack.Pack;
import com.goodworkalan.strata.Strata;

/**
 * A container that maps a strata to the pack file that stores it.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the b+tree objects.
 */
public class PackStrata<T> extends PackFile
{
    /** The strata. */
    private final Strata<T> strata;

    /**
     * Create a strata mapping from the given pack, file and strata.
     * 
     * @param file
     *            The pack file.
     * @param pack
     *            The pack that contains the strata.
     * @param strata
     *            The strata.
     */
    public PackStrata(File file, Pack pack, Strata<T> strata)
    {
        super(file, pack);
        this.strata = strata;
    }
    
    /**
     * Get the strata.
     * 
     * @return The strata.
     */
    public Strata<T> getStrata()
    {
        return strata;
    }
}
