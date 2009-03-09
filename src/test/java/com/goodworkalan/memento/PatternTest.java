package com.goodworkalan.memento;

import static org.testng.Assert.assertEquals;

import java.io.File;

import org.testng.annotations.Test;

import com.goodworkalan.memento.serializable.Address;
import com.goodworkalan.memento.serializable.Author;
import com.goodworkalan.memento.serializable.Book;
import com.goodworkalan.memento.serializable.Person;

public class PatternTest
{
    @Test
    public void store() 
    {
        Person person = new Person();

        Store store = new Store(new FileStorage(new File("test.memento")));
        
        store
            .store(Person.class)
                .subclass(Author.class)
                .io(SerializationIO.getInstance(Person.class))
                .index(new Index<String>("email") {})
                    .indexer(new Indexer<Person, String>()
                    {
                        public String index(Person object)
                        {
                            return object.getEmail();
                        }
                    })
                    .end()
                .index(new Index<Ordered>("lastNameFirst") {})
                    .indexer(new Indexer<Person, Ordered>()
                    {
                        public Ordered index(Person object)
                        {
                            return new Ordered(object.getLastName(), object.getFirstName());
                        }
                    })
                    .end()
                .end()
            .link(new Link().bin(Person.class).bin(Book.class))
                .alternate(new Link().bin(Book.class).bin(Person.class))
                .end()
            .link(new Link("children").bin(Person.class).bin(Person.class))
                .alternate(new Link("parent").bin(Person.class).bin(Person.class))
                .end()
            .end();
                
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
        
        // FIXME Serialize the store? What's the point? If the code changes, it 
        // changes, and the serialized objects will disappear, especially if 
        // they are inline.
        snapshot.bin(Person.class);
        
        person.setFirstName("Steve");
        snapshot.bin(Person.class).update(person);

        Bin<Person> bin = snapshot.bin(Person.class);
        assertEquals(bin.get(key).getFirstName(), "Steve");
        
        snapshot.commit();
        
        person = snapshot.bin(Person.class).get(snapshot.bin(Person.class).key(person));

        person.setLastName("Gutierrez");
        snapshot.bin(Person.class).replace(person, person);
        
        for (Person each : snapshot.bin(Person.class).getAll())
        {
            System.out.println(each);
        }
        
        Address address = new Address();
        address.setZip("70119");
        
        snapshot
            .join(new Link().bin(Person.class).bin(Book.class))
            .push(Person.class, person)
            .push(Book.class, new Book("My Diary"))
            .add();
        
        Bin<Person> people = snapshot.bin(Person.class);
        people.join(person, Book.class).add(new Book("War and Peace"));
        
        snapshot.commit();
    }
}
