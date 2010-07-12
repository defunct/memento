package com.goodworkalan.memento;

/**
 * A block that mutates the stored objects.
 *
 * @author Alan Gutierrez
 */
public interface Mutation {
    /**
     * Commit the mutation defined using the mutator.
     * 
     * @param mutator
     *            The mutator.
     */
    public void commit(Mutator mutator);
}
