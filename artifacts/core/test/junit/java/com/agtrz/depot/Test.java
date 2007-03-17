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

import com.agtrz.depot.Circle;
import com.agtrz.depot.Person;
import com.agtrz.depot.Storage;
import com.agtrz.depot.Storage.RecordReference;

import junit.framework.Assert;

public class Test
{
    public final static class Environment
    {
        public final File file;

        public final Random random;

        public final Map mapOfIdentifiers;

        public Storage storage;

        public Storage.Mutator mutator;

        public int objectCount;

        public Environment(File file, Storage storage)
        {
            this.file = file;
            this.random = new Random();
            this.storage = storage;
            this.mutator = storage.mutate();
            this.mapOfIdentifiers = new HashMap();
        }

        public void reopen()
        {
            this.storage.close();
            this.storage = new Storage.Opener().open(file);
            this.mutator = storage.mutate();
        }
    }

    public final static class ObjectAllocation
    {
        public final String bagName;

        public final Long key;

        public final Object object;

        public final Map mapOfRelationships;

        public ObjectAllocation(String bagName, Long key, Object object)
        {
            this.bagName = bagName;
            this.key = key;
            this.object = object;
            this.mapOfRelationships = new HashMap();
        }

        public void relate(String relationshipName, RecordReference reference)
        {
            Set setOfObjects = (Set) mapOfRelationships.get(relationshipName);
            if (setOfObjects == null)
            {
                setOfObjects = new HashSet();
                mapOfRelationships.put(relationshipName, setOfObjects);
            }
            setOfObjects.add(reference);
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
            Storage.Bag bag = environment.mutator.getBag(bagName);
            Object object = newObject.get();
            Storage.Record record = bag.add(object);
            environment.mapOfIdentifiers.put(new Integer(environment.objectCount++), new ObjectAllocation(bagName, record.getReference().getKey(), object));
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
            Storage.Join join = env.mutator.getJoin(joinName);
            ObjectAllocation left = (ObjectAllocation) env.mapOfIdentifiers.get(new Integer(objectCountOne));
            ObjectAllocation right = (ObjectAllocation) env.mapOfIdentifiers.get(new Integer(objectCountTwo));
            Storage.Record keptLeft = env.mutator.getBag(left.bagName).get(left.key);
            Storage.Record keptRight = env.mutator.getBag(right.bagName).get(right.key);
            join.add(keptLeft, keptRight);
            left.relate(joinName, keptRight.getReference());
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
            ObjectAllocation alloc = (ObjectAllocation) environment.mapOfIdentifiers.get(new Integer(objectNumber));
            Storage.Bag bag = environment.mutator.getBag(alloc.bagName);
            Storage.Record keptObject = bag.get(alloc.key);
            Assert.assertEquals(alloc.object, keptObject.getObject());
            Iterator relationships = alloc.mapOfRelationships.entrySet().iterator();
            while (relationships.hasNext())
            {
                Map.Entry entry = (Map.Entry) relationships.next();
                String name = (String) entry.getKey();
                Set setOfObjects = (Set) entry.getValue();
                Storage.Join join = environment.mutator.getJoin(name);
                int count = 0;
                Iterator found = join.find(keptObject);
                while (found.hasNext())
                {
                    Storage.Record[] records = (Storage.Record[]) found.next();
                    if (!records[0].getObject().equals(keptObject.getObject()))
                    {
                        break;
                    }
                    Assert.assertTrue(setOfObjects.contains(records[1].getReference()));
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
            ObjectAllocation alloc = (ObjectAllocation) environment.mapOfIdentifiers.get(new Integer(objectCount));
            Storage.Bag bag = environment.mutator.getBag(alloc.bagName);
            Storage.Record keptObject = bag.get(alloc.key);
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