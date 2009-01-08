package com.goodworkalan.memento;


public interface Indexer<T, F>
{
    public F index(T object);
}