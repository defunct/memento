package com.goodworkalan.memento;

import com.goodworkalan.ilk.Ilk;


public class BinBuilder<T>
{
    private final Store store;
    
    private final Ilk<T> ilk;
    
    private final BinSchemaTable binSchemas;
    
    public BinBuilder(Store store, BinSchemaTable binSchemas, Ilk<T> ilk)
    {
        this.store = store;
        this.ilk = ilk;
        this.binSchemas = binSchemas;
    }
    
    public <S extends T> BinBuilder<T> subclass(Class<S> subclass)
    {
        return this;
    }
    
    public BinBuilder<T> io(ItemIO<T> io)
    {
        binSchemas.get(ilk).setItemIO(io);
        return this;
    }
    
    public <F extends Comparable<? super F>> IndexBuilder<T, F> index(Index<F> index)
    {
        return new IndexBuilder<T, F>(this, binSchemas, ilk, index);
    }
    
    public Store end()
    {
        return store;
    }
}
