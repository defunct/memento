package com.goodworkalan.memento;

import com.goodworkalan.ilk.Ilk;

/**
 * An embedded domain specific language that sets the properties of a 
 * bin.
 *  
 * @author Alan Gutierrez
 *
 * @param <T> The type of object stored in the bin.
 */
public class BinBuilder<T>
{
    /** The database. */
    private final Store store;
    
    /** The super type token of the objects stored in the bin. */
    private final Ilk<T> ilk;
    
    // TODO Document.
    private final BinSchemaTable binSchemas;
    
    // TODO Document.
    public BinBuilder(Store store, BinSchemaTable binSchemas, Ilk<T> ilk)
    {
        this.store = store;
        this.ilk = ilk;
        this.binSchemas = binSchemas;
    }
    
    // TODO Document.
    public <S extends T> BinBuilder<T> subclass(Class<S> subclass)
    {
        return this;
    }
    
    // TODO Document.
    public BinBuilder<T> io(ItemIO<T> io)
    {
        binSchemas.get(ilk).setItemIO(io);
        return this;
    }
    
    // TODO Document.
    public <F extends Comparable<? super F>> IndexBuilder<T, F> index(Class<F> meta)
    {
        return index(new Ilk<F>(meta), "");
    }

    // TODO Document.
    public <F extends Comparable<? super F>> IndexBuilder<T, F> index(Class<F> meta, String name)
    {
        return index(new Ilk<F>(meta), name);
    }

    // TODO Document.
    public <F extends Comparable<? super F>> IndexBuilder<T, F> index(Ilk<F> order)
    {
        return index(order, "");
    }

    // TODO Document.
    public <F extends Comparable<? super F>> IndexBuilder<T, F> index(Ilk<F> order, String name)
    {
        return new IndexBuilder<T, F>(this, binSchemas, ilk, new Index<F>(order, name));
    }
    
    // TODO Document.
    public Store end()
    {
        return store;
    }
}
