package com.goodworkalan.memento;

import java.io.Serializable;

public interface Indexer<T>
extends Serializable
{
    public String[] getNames();

    public Comparable<?>[] extract(T type);
}