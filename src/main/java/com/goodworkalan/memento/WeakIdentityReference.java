package com.goodworkalan.memento;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * A weak identity reference used to track owner instances created by
 * injection for nested instances. This object defines equality for use in a
 * map using first the identity of the referenced itself, then the identity
 * of the referenced object, so that the reference itself can be used to
 * remove the entry from the map, after the object has been collected.
 * 
 * @author Alan Gutierrez
 */
final class WeakIdentityReference extends WeakReference<Object> {
    /** Cache the hash code of the underlying object. */
    private final int hashCode;

    /**
     * Create a weak identity reference for the given object.
     * 
     * @param object
     *            The object to reference.
     * @param queue
     *            The reference queue used to reap entries.
     */
    public WeakIdentityReference(Object object, ReferenceQueue<Object> queue) {
        super(object, queue);
        this.hashCode = System.identityHashCode(object);
    }

    /**
     * Get the identity hash code of the referenced object.
     * 
     * @return The hash code.
     */
    public int hashCode() {
        return hashCode;
    }

    /**
     * This reference is equal to the given object if it is the same
     * instance as this object, or if the referenced objects are the the
     * same objects.
     * 
     * @param object
     *            The object to test for equality.
     * @return True if this object is equal to the given object.
     */
    public boolean equals(Object object) {
        // The test against this will short circuit the dereference when we
        // collect.
        return object == this
                || get() == ((WeakIdentityReference) object).get();
    }
}