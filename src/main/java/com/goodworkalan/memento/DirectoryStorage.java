package com.goodworkalan.memento;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Map;

import com.goodworkalan.ilk.UncheckedCast;
import com.goodworkalan.pack.Creator;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Opener;
import com.goodworkalan.pack.Pack;

public class DirectoryStorage extends AbstractStorage<String>
{
    private File dir;
    
    public DirectoryStorage(File dir)
    {
        this.dir = dir;
    }

    @Override
    protected StrataPointer open(String file)
    {
        Pack pack;
        Opener opener = new Opener();
        try
        {
            opener.open(new RandomAccessFile(new File(dir, file), "rw").getChannel());
            pack = opener.getPack();
        }
        catch (FileNotFoundException e)
        {
            throw new MementoException(0, e);
        }
        Mutator mutator = pack.mutate();
        ByteBuffer bytes = mutator.read(mutator.getPack().getStaticBlocks().get(HEADER_URI));
        long address = bytes.getLong();
        mutator.commit();
        return new StrataPointer(new File(dir, file), pack, address);
    }
    
    @Override
    protected StrataPointer create()
    {
        Creator creator = new Creator();
        creator.addStaticBlock(HEADER_URI, Long.SIZE / Byte.SIZE);
        File file;
        try
        {
            file = File.createTempFile("storage", ".pack", dir);
        }
        catch (IOException e)
        {
            throw new MementoException(0, e);
        }
        Pack pack;
        try
        {
            pack = creator.create(new RandomAccessFile(file, "rw").getChannel());
        }
        catch (IOException e)
        {
            throw new MementoException(0, e);
        }
        return new StrataPointer(file, pack, 0L);
    }
    
    @Override
    protected String record(StrataPointer pointer, long address, Mutator mutator)
    {
        File schema = new File(dir, "schema");
        try
        {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(schema));
            out.writeObject(mapOfBins);
            out.writeObject(mapOfIndexes);
            out.writeObject(mapOfJoins);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return pointer.getFile().getName();
    }
    
    public void open()
    {
        try
        {
            tryOpen();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    public void tryOpen() throws IOException
    {
        File schema = new File(dir, "schema");
        mapOfBins.clear();
        mapOfIndexes.clear();
        mapOfJoins.clear();
        if (schema.exists())
        {
            ObjectInputStream in = new ObjectInputStream(new FileInputStream(schema));
            try
            {
                mapOfBins.putAll(new UncheckedCast<Map<Item<?>, String>>().cast(in.readObject()));
                mapOfIndexes.putAll(new UncheckedCast<Map<Map<Item<?>, Index<?>>, String>>().cast(in.readObject()));
                mapOfJoins.putAll(new UncheckedCast<Map<Link, String>>().cast(in.readObject()));
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }
            in.close();
        }
    }
}
