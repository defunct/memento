package com.goodworkalan.memento;

class UnsafeCast<T> implements Caster<T>
{
    @SuppressWarnings("unchecked")
    public T cast(Object object)
    {
        return (T) object;
    }
}
