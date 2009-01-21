package com.goodworkalan.memento;


public class BinBuilder<T>
{
    private final Store store;
    
    private final Item<T> item;
    
    private final BinSchemaTable binSchemas;
    
    public BinBuilder(Store store, BinSchemaTable binSchemas, Item<T> item)
    {
        this.store = store;
        this.item = item;
        this.binSchemas = binSchemas;
    }
    
    public <S extends T> BinBuilder<T> subclass(Class<S> subclass)
    {
        return this;
    }
    
    public BinBuilder<T> io(ItemIO<T> io)
    {
        binSchemas.get(item).setItemIO(io);
        return this;
    }
    
    public <F extends Comparable<? super F>> IndexBuilder<T, F> index(Index<F> index)
    {
        return new IndexBuilder<T, F>(this, binSchemas, item, index);
    }
    
    public Store end()
    {
        return store;
    }
}
