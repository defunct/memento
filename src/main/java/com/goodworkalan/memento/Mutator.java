package com.goodworkalan.memento;

import static com.goodworkalan.memento.Store._;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Expresses a mutation of stored objects.
 *
 * @author Alan Gutierrez
 */
public class Mutator {
    /** The store. */
    private final Store store;

    /**
     * Create a mutator for the given store.
     * 
     * @param store
     *            The store.
     */
    public Mutator(Store store) {
        this.store = store;
    }
    
    /** The map of uncreated gatekeepers. */
    private final Map<Object, Boolean> newGatekeepers = new IdentityHashMap<Object, Boolean>();

    /** Object that we have already mutate. */
    private final Map<Object, Boolean> seen = new IdentityHashMap<Object, Boolean>();
    
    /** The map of mutations for a particular gatekeeper. */
    private final Map<Object, Object> script = new IdentityHashMap<Object, Object>();
    
    /** The map of objects to their versions. */
    private final Map<Object, Long> versions = new IdentityHashMap<Object, Long>();

    /**
     * Create a new gatekeeper object.
     * 
     * @param object The object to create in the data store.
     */
    public void create(Object object) {
        newGatekeepers.put(object, true);
    }
    
    /** Determine if the object is managed. */
    public void gatekeeper(Object object) {
        if (!script.containsKey(object)) {
            if (!newGatekeepers.containsKey(object)) {
                if (!store.isManaged(object)) {
                    throw new IllegalArgumentException(_(Mutator.class, "notGatekeeper"));
                }
                versions.put(object, store.refresh(object));
            }
        }
    }
    
    /**
     * Create a new object using the given gatekeeper object.
     * 
     * @param gatekeeper
     *            The gatekeeper.
     * @param object
     *            The object to create.
     */
    public void create(Object gatekeeper, Object object) {
        gatekeeper(gatekeeper);
    }
}
