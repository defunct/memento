package com.goodworkalan.memento;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.ilk.UncheckedCast;
import com.goodworkalan.pack.Creator;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Opener;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.pack.io.ByteBufferInputStream;
import com.goodworkalan.pack.io.PackOutputStream;

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
            creator.addStaticBlock(HEADER_URI, Long.SIZE / Byte.SIZE);
            pack = creator.create(new RandomAccessFile(file, "rw").getChannel());

            Mutator mutator = pack.mutate();
            
            long header = mutator.getPack().getStaticBlocks().get(HEADER_URI);
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
        
        Opener opener = new Opener();
        opener.open(new RandomAccessFile(file, "rw").getChannel());
        pack = opener.getPack(); 

        Mutator mutator = pack.mutate();
        
        ByteBuffer header = mutator.read(mutator.getPack().getStaticBlocks().get(HEADER_URI)); 
        
        ByteBuffer bytes = mutator.read(header.getLong());
        ObjectInputStream in = new ObjectInputStream(new ByteBufferInputStream(bytes));
        mapOfBins.putAll(new UncheckedCast<Map<Item<?>, Long>>().cast(in.readObject()));
        mapOfIndexes.putAll(new UncheckedCast<Map<Map<Item<?>, Index<?>>, Long>>().cast(in.readObject()));
        mapOfJoins.putAll(new UncheckedCast<Map<Link, Long>>().cast(in.readObject()));
        
        in.close();
        
        mutator.commit();
    }
 
    protected StrataPointer open(Long address)
    {
        return new StrataPointer(file, pack, address);
    }
    
    @Override
    protected StrataPointer create()
    {
        return new StrataPointer(file, pack, 0L);
    }
    
    @Override
    protected Long record(StrataPointer pointer, long address, Mutator mutator)
    {
        long header = mutator.getPack().getStaticBlocks().get(HEADER_URI);
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
