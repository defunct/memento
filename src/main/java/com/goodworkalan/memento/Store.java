package com.goodworkalan.memento;

import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Collections;

import com.goodworkalan.favorites.Stash;

public class Store
{
    public <Item> BinBuilder<Item> store(Class<Item> itemClass)
    {
        return new BinBuilder<Item>(itemClass);
    }
    
    public Snapshot newSnapshot(Sync sync)
    {
        return null;
    }
    
    public Snapshot newSnapshot()
    {
        return null;
    }
    
    public <Item> long add(Item item)
    {
        return 0L;
    }
    
    public <Item> long getId(Item item)
    {
        return 0L;
    }
    
    public <Item> Item get(Class<Item> itemClass, long id)
    {
        return null;
    }

    public <Item> Collection<Item> getAll(Class<Item> itemClass)
    {
        return Collections.emptyList();
    }
    
    public <From, To> OneToMany<From, To> toMany(From from, Class<To> to)
    {
        return new OneToMany<From, To>();
    }
    
    public <From, To> OneToMany<From, To> toMany(From from, Class<To> to, String name)
    {
        return new OneToMany<From, To>();
    }

    public abstract static class Type<T>
    {
        private final java.lang.reflect.Type type;
        
        public Type()
        {
            // Give me class information.  
            Class<?> klass = getClass();  
           
            // See that I have created an anonymous subclass of TypeReference in the  
            // main method. Hence, to get the TypeReference itself, I need superclass.  
            // Furthermore, to get Type information, you should call  
            // getGenericSuperclass() instead of getSuperclass().  
            java.lang.reflect.Type superClass = klass.getGenericSuperclass();  
           
            if (superClass instanceof Class)
            {  
                // Type has four subinterface:  
                // (1) GenericArrayType: component type is either a  
                // parameterized type or a type variable. Parameterized type is a class  
                // or interface with its actual type argument, e.g., ArrayList<String>.  
                // Type variable is unqualified identifier like T or V.  
                //  
                // (2) ParameterizedType: see (1).  
                //  
                // (3) TypeVariable<D>: see (1).  
                //  
                // (4) WildcardType: ?  
                //  
                // and one subclass:  
                // (5) Class.  
                //  
                // If TypeReference is created by 'new TypeReference() { }', then  
                // superClass would be just an instance of Class instead of one of the  
                // interfaces described above. In that case, because I don't have type  
                // passed to TypeReference, an exception should be raised.  
                throw new RuntimeException("Missing Type Parameter");  
            }  
           
            // By superClass, we mean 'TypeReference<T>'. So, it is obvious that  
            // superClass is ParameterizedType.  
            ParameterizedType pt = (ParameterizedType) superClass;  
           
            // We have one type argument in TypeRefence<T>: T.  
            type = pt.getActualTypeArguments()[0];  
        }
        
        public java.lang.reflect.Type getType()
        {
            return type;
        }
    }
}
