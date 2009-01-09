package com.goodworkalan.memento;

import static org.testng.Assert.assertEquals;

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
        
        person.setFirstName("Alan");
        person.setLastName("Gutierrez");
        person.setEmail("alan@blogometer.com");
        
        Snapshot snapshot = store.newSnapshot();
        
        long key = snapshot.bin(Person.class).add(person);
        
        snapshot.commit();
        
        snapshot = store.newSnapshot();
        
        person = snapshot.bin(Person.class).get(key);

        assertEquals(person.getFirstName(), "Alan");
        assertEquals(person.getLastName(), "Gutierrez");
        assertEquals(person.getEmail(), "alan@blogometer.com");
        
        snapshot.commit();
        
        snapshot = store.newSnapshot();
        
        // FIXME Serialize? What's the point? If the code changes, it 
        // changes, and the serialized objects will disappear, especially
        // if they are inline.
        snapshot.bin(Person.class);
        
        person.setFirstName("Steve");
        snapshot.bin(Person.class).update(person);
        snapshot.update(Person.class, person);

        Bin<Person> bin = snapshot.bin(Person.class);
        assertEquals(bin.get(key).getFirstName(), "Steve");
        
        snapshot.commit();
        
        person = store.get(Person.class, store.getId(person));

        person.setLastName("Gutierrez");
        snapshot.bin(Person.class).replace(person, person);
        
        for (Person each : store.getAll(Person.class))
        {
            System.out.println(each);
        }
        
        Address address = new Address();
        address.setZip("70119");
        
        store.toMany(person, Address.class).add(address);
    }
}
