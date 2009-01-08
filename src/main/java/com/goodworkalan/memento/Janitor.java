package com.goodworkalan.memento;

import java.io.Serializable;

import com.goodworkalan.pack.Mutator;

public interface Janitor
extends Serializable
{
    public void rollback(Snapshot snapshot);

    public void dispose(Mutator mutator, boolean deallocate);
}