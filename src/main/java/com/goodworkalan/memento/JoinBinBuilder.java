package com.goodworkalan.memento;

public class JoinBinBuilder<T, R>
{
    public <N> JoinBinBuilder<N, JoinBinBuilder<T, R>> bin(Class<N> itemClass)
    {
        return new JoinBinBuilder<N, JoinBinBuilder<T,R>>();
    }
    
    public <P> Link<T, P> reverse(Class<P> remainClass)
    {
        return new Link<T, P>();
    }
}
