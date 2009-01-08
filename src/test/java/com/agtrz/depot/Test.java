/* Copyright Alan Gutierrez 2006 */
package com.agtrz.depot;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.agtrz.depot.serializable.Person;
import com.goodworkalan.memento.Bag;
import com.goodworkalan.memento.Bin;
import com.goodworkalan.memento.SerializationUnmarshaller;
import com.goodworkalan.memento.Snapshot;
import com.goodworkalan.memento.Unmarshaller;

import junit.framework.Assert;

public class Test
{
    public final static class Environment
    {
        public final File file;

        public final Random random;

        public final Map<Integer, ObjectAllocation> mapOfIdentifiers;

        public Depot storage;

        public Snapshot mutator;

        public int objectCount;

        public Environment(File file, Depot storage)
        {
            this.file = file;
            this.random = new Random();
            this.storage = storage;
            this.mutator = storage.newSnapshot();
            this.mapOfIdentifiers = new HashMap<Integer, ObjectAllocation>();
        }

        public void reopen()
        {
            this.storage.close();
            this.storage = new Opener().open(file);
            this.mutator = storage.newSnapshot();
        }
    }

    public final static class ObjectAllocation
    {
        public final String bagName;

        public final Long key;

        public final Object object;

        public final Map<String, Set<Object>> mapOfRelationships;

        public ObjectAllocation(String bagName, Long key, Object object)
        {
            this.bagName = bagName;
            this.key = key;
            this.object = object;
            this.mapOfRelationships = new HashMap<String, Set<Object>>();
        }

        public void relate(String joinName, Long bag)
        {
            Set<Object> setOfObjects = mapOfRelationships.get(joinName);
            if (setOfObjects == null)
            {
                setOfObjects = new HashSet<Object>();
                mapOfRelationships.put(joinName, setOfObjects);
            }
            setOfObjects.add(bag);
        }
    }

    public interface ObjectServer
    {
        public Object get();
    }

    public final static class PersonServer
    implements ObjectServer, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final String firstName;

        private final String lastName;

        public PersonServer(String firstName, String lastName)
        {
            this.firstName = firstName;
            this.lastName = lastName;
        }

        public Object get()
        {
            Person person = new Person();
            person.setFirstName(firstName);
            person.setLastName(lastName);
            return person;
        }
    }

    public final static class CircleServer
    implements ObjectServer, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final String name;

        public CircleServer(String name)
        {
            this.name = name;
        }

        public Object get()
        {
            return new Circle(name);
        }
    }

    public final static class Add
    implements Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final ObjectServer newObject;

        private final String bagName;

        public Add(ObjectServer newObject, String bagName)
        {
            this.newObject = newObject;
            this.bagName = bagName;
        }

        public void operate(Environment environment)
        {
            Depot.Bin bin = environment.mutator.getBin(bagName);
            Object object = newObject.get();
            Bag bag = bin.add(object);
            environment.mapOfIdentifiers.put(new Integer(environment.objectCount++), new ObjectAllocation(bagName, bag.getKey(), object));
        }
    }

    public final static class Join
    implements Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final String joinName;

        private final int objectCountOne;

        private final int objectCountTwo;

        public Join(String joinName, int objectCountOne, int objectCountTwo)
        {
            this.joinName = joinName;
            this.objectCountOne = objectCountOne;
            this.objectCountTwo = objectCountTwo;
        }

        public void operate(Environment env)
        {
            Join join = env.mutator.getJoin(joinName);
            ObjectAllocation left = (ObjectAllocation) env.mapOfIdentifiers.get(new Integer(objectCountOne));
            ObjectAllocation right = (ObjectAllocation) env.mapOfIdentifiers.get(new Integer(objectCountTwo));

            Map<String, Long> mapOfKeys = new HashMap<String, Long>();

            mapOfKeys.put(left.bagName, left.key);
            mapOfKeys.put(right.bagName, right.key);

            join.link(mapOfKeys);

            left.relate(joinName, right.key);
        }
    }

    public final static class Get
    implements Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final int objectNumber;

        public Get(int objectNumber)
        {
            this.objectNumber = objectNumber;
        }

        public void operate(Environment environment)
        {
            Unmarshaller unmarshaller = new SerializationUnmarshaller();
            ObjectAllocation alloc = (ObjectAllocation) environment.mapOfIdentifiers.get(new Integer(objectNumber));
            Depot.Bin bin = environment.mutator.getBin(alloc.bagName);
            Bag keptObject = bin.get(unmarshaller, alloc.key);
            Assert.assertEquals(alloc.object, keptObject.getObject());
            for (Map.Entry<String, Set<Object>> entry : alloc.mapOfRelationships.entrySet())
            {
                // String name = (String) entry.getKey();
                Set<Object> setOfObjects = entry.getValue();
                int count = 0;
                Iterator<Object> found = null; // keptObject.getLinked(name);
                while (found.hasNext())
                {
                    Bag[] records = (Bag[]) found.next();
                    if (!records[0].getObject().equals(keptObject.getObject()))
                    {
                        break;
                    }
                    Assert.assertTrue(setOfObjects.contains(records[1].getKey()));
                    count++;
                }
                Assert.assertEquals(count, setOfObjects.size());
            }
        }
    }

    public final static class RolledBack
    implements Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final int objectCount;

        public RolledBack(int objectNumber)
        {
            this.objectCount = objectNumber;
        }

        public void operate(Environment environment)
        {
            Unmarshaller unmarshaller = new SerializationUnmarshaller();
            ObjectAllocation alloc = (ObjectAllocation) environment.mapOfIdentifiers.get(new Integer(objectCount));
            Depot.Bin bag = environment.mutator.getBin(alloc.bagName);
            Bag keptObject = bag.get(unmarshaller, alloc.key);
            Assert.assertNull(keptObject);
            environment.mapOfIdentifiers.remove(new Integer(objectCount));
        }
    }

    public final static class Commit
    implements Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public void operate(Environment env)
        {
            env.mutator.commit();
        }
    }

    public final static class Rollback
    implements Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public void operate(Environment env)
        {
            env.mutator.rollback();
        }
    }

}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */