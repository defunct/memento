package com.goodworkalan.memento;

public class BinRedux<T>
{
    public <F> BinRedux<T> createIndex(Indexer<T, F> indexer, String name)
    {
        return this;
    }
}
