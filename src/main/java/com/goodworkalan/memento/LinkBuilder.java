package com.goodworkalan.memento;

public class LinkBuilder
{
    private Store store;
    
    public LinkBuilder(Store store)
    {
        this.store = store;
    }
    
    public Store end()
    {
        return store;
    }
}
