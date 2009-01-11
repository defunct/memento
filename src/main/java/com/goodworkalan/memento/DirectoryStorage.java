package com.goodworkalan.memento;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

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
        Pack pack = new Opener().open(new File(dir, file));
        Mutator mutator = pack.mutate();
        ByteBuffer bytes = mutator.read(mutator.getSchema().getStaticPageAddress(HEADER_URI));
        long address = bytes.getLong();
        mutator.commit();
        return new StrataPointer(pack, address);
    }
    
    @Override
    protected StrataPointer create()
    {
        Creator creator = new Creator();
        creator.addStaticPage(HEADER_URI, Pack.ADDRESS_SIZE);
        Pack pack;
        try
        {
            pack = creator.create(File.createTempFile("storage", ".pack", dir));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return new StrataPointer(pack, 0L);
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
        return pointer.getPack().getFile().getName();
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
                mapOfBins.putAll(new UnsafeCast<Map<Item<?>, String>>().cast(in.readObject()));
                mapOfIndexes.putAll(new UnsafeCast<Map<Map<Item<?>, Index<?>>, String>>().cast(in.readObject()));
                mapOfJoins.putAll(new UnsafeCast<Map<Link, String>>().cast(in.readObject()));
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }
            in.close();
        }
    }
}
