package com.goodworkalan.memento;

import java.lang.reflect.ParameterizedType;

public class Store
{
    private final BinSchemaTable binSchemas = new BinSchemaTable();
    
    public <T> BinBuilder<T> store(Class<T> itemClass)
    {
        return new BinBuilder<T>(this, binSchemas, new Item<T>(itemClass) {});
    }
    
    public <T> BinBuilder<T> store(Item<T> item)
    {
        return new BinBuilder<T>(this, binSchemas, item);
    }
    
    public LinkBuilder link(Link link)
    {
        return null;
    }

    public Snapshot newSnapshot(Sync sync)
    {
        return null;
    }
    
    public Snapshot newSnapshot()
    {
        return null;
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
