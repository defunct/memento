package com.goodworkalan.memento;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.pack.Creator;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Opener;
import com.goodworkalan.pack.Pack;

public class FileStorage extends AbstractStorage<Long>
{
    private final File file;
    
    private Pack pack;
    
    public FileStorage(File file)
    {
        this.file = file;
    }

    public void open()
    {
        try
        {
            tryOpen();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
    public void tryOpen() throws IOException, ClassNotFoundException
    {
        mapOfBins.clear();
        mapOfJoins.clear();
        mapOfIndexes.clear();

        if (!file.exists())
        {
            Creator creator = new Creator();
            creator.addStaticPage(HEADER_URI, Pack.ADDRESS_SIZE);
            pack = creator.create(file);

            Mutator mutator = pack.mutate();
            
            long header = mutator.getSchema().getStaticPageAddress(HEADER_URI);
            ByteBuffer bytes = mutator.read(header); 

            PackOutputStream allocation = new PackOutputStream(mutator);
            
            ObjectOutputStream out = new ObjectOutputStream(allocation);
            out.writeObject(new HashMap<Item<?>, Long>());
            out.writeObject(new HashMap<Map<Item<?>, Index<?>>, Long>());
            out.writeObject(new HashMap<Link, Long>());
            out.close();
            
            bytes.putLong(allocation.allocate());
            bytes.flip();
            mutator.write(header, bytes);
            
            mutator.commit();
            
            pack.close();
        }
        
        pack = new Opener().open(file);

        Mutator mutator = pack.mutate();
        
        ByteBuffer header = mutator.read(mutator.getSchema().getStaticPageAddress(HEADER_URI)); 
        
        ByteBuffer bytes = mutator.read(header.getLong());
        ObjectInputStream in = new ObjectInputStream(new ByteBufferInputStream(bytes));
        mapOfBins.putAll(new UnsafeCast<Map<Item<?>, Long>>().cast(in.readObject()));
        mapOfIndexes.putAll(new UnsafeCast<Map<Map<Item<?>, Index<?>>, Long>>().cast(in.readObject()));
        mapOfJoins.putAll(new UnsafeCast<Map<Link, Long>>().cast(in.readObject()));
        
        in.close();
        
        mutator.commit();
    }
 
    protected StrataPointer open(Long address)
    {
        return new StrataPointer(pack, address);
    }
    
    @Override
    protected StrataPointer create()
    {
        return new StrataPointer(pack, 0L);
    }
    
    @Override
    protected Long record(StrataPointer pointer, long address, Mutator mutator)
    {
        long header = mutator.getSchema().getStaticPageAddress(HEADER_URI);
        ByteBuffer bytes = mutator.read(header);

        mutator.free(bytes.getLong());
        bytes.flip();

        PackOutputStream allocation = new PackOutputStream(mutator);
        
        try
        {
            ObjectOutputStream out = new ObjectOutputStream(allocation);
            out.writeObject(mapOfBins);
            out.writeObject(mapOfIndexes);
            out.writeObject(mapOfJoins);
            out.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        
        bytes.putLong(allocation.allocate());
        bytes.flip();
        mutator.write(header, bytes);
        
        return address;
    }
}
