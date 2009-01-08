package com.goodworkalan.memento;

import org.testng.annotations.Test;

import com.agtrz.depot.serializable.Address;
import com.agtrz.depot.serializable.Employee;
import com.agtrz.depot.serializable.Person;

public class PatternTest
{
    @Test
    public void store() 
    {
        Person person = new Person();

        Store store = new Store();
        
        store
            .store(Person.class)
                .subclass(Employee.class)
                .io(SerializationIO.getInstance(Person.class))
                .index("email", new Indexer<Person, String>()
                {
                    public String index(Person object)
                    {
                        return object.getEmail();
                    }
                })
                .index("lastNameFirst", new Indexer<Person, Ordered>()
                {
                    public Ordered index(Person object)
                    {
                        return new Ordered(object.getLastName(), object.getFirstName());
                    }
                });
        
        store.add(person);
        
        // FIXME Serialize? What's the point? If the code changes, it 
        // changes, and the serialized objects will disappear, especially
        // if they are inline.
        store.bin(Person.class);
        
        person.setFirstName("Alan");
        store.update(person);
        
        person = store.get(Person.class, store.getId(person));

        person.setLastName("Gutierrez");
        store.replace(person, person);
        
        for (Person each : store.getAll(Person.class))
        {
            System.out.println(each);
        }
        
        Address address = new Address();
        address.setZip("70119");
        
        store.toMany(person, Address.class).add(address);
    }
}
