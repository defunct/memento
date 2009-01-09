package com.goodworkalan.memento;

public class IndexBuilder<Item, Fields>
{
    private final String name;
    
    private final Indexer<Item, Fields> indexer;
    
    public IndexBuilder(String name, Indexer<Item, Fields> indexer)
    {
        this.name = name;
        this.indexer = indexer;
    }
}
