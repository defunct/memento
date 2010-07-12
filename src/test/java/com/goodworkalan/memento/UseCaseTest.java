package com.goodworkalan.memento;

import java.io.File;

import org.testng.annotations.Test;

import com.goodworkalan.memento.serializable.Address;
import com.goodworkalan.memento.serializable.Person;

/**
 * General tests to determine functionality.
 *
 * @author Alan Gutierrez
 */
public class UseCaseTest {
    /** Noodleing. */
    @Test
    public void noodle() {
        SerializerConfiguration configuration = new SerializerConfiguration();
        configuration.dataDirectory = new File(new File(".").getAbsoluteFile(), "target/database");
        configuration.dataDirectory.mkdirs();
        Serializer serializer = new Serializer(configuration);
        serializer.start();
        Store store = new Store(serializer);
        store.commit(new Mutation() {
            public void commit(Mutator mutation) {
                Person person = new Person();
                person.firstName = "Dwight";
                person.lastName = "Eisenhower";
                person.email = "dwight@whitehouse.gov";
                mutation.create(person);
                Address address = new Address();
                address.state = "1600 Pennsylvania Ave";
                address.city = "Wasington D.C.";
                address.state = "DC";
                mutation.create(person, address);
//                mutation.associate(person, person, address);
                // Need some way to make the email address indexing also atomic.
            }
        });
        serializer.stop();
    }
}
