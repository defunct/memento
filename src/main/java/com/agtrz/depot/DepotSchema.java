package com.agtrz.depot;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.agtrz.depot.Depot.Index;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Pack;

public class DepotSchema
{
    private final Pack pack;
    
    private final Map<Class<?>, BinSchema> mapOfBinSchemas;
    
    private final Map<Long, BinHeader> mapOfBinHeaders;
        
    private final Marshaller marshaller;
    
    private final Unmarshaller unmarshaller;
    
    public DepotSchema(Pack pack)
    {
        this.pack = pack;
        this.mapOfBinSchemas = new HashMap<Class<?>, BinSchema>();
        this.mapOfBinHeaders = new HashMap<Long, BinHeader>();
        this.marshaller = new SerializationMarshaller();
        this.unmarshaller = new SerializationUnmarshaller();
    }
    
    public Marshaller getMarshaller()
    {
        return marshaller;
    }
    
    public Unmarshaller getUnmarshaller()
    {
        return unmarshaller;
    }

    public synchronized BinSchema getBinSchema(Class<?> klass)
    {
        try
        {
            return tryGetBinSchema(klass);
        }
        catch (IOException e)
        {
            throw new Danger("Hello.", e, 100);
        }
    }
    
    public BinSchema tryGetBinSchema(Class<?> klass) throws IOException
    {
        BinSchema schema = mapOfBinSchemas.get(klass);
        if (schema == null)
        {
            Mutator mutator = pack.mutate();
            Strata strata = new BinTree().create(mutator).getStrata();
            
            Bin.Header header = new Bin.Header(strata);
            
            PackOutputStream allocator = new PackOutputStream(mutator);
            
            ObjectOutputStream out = new ObjectOutputStream(allocator);
            out.writeObject(header);
            out.close();
            
            long address = allocator.allocate();
            
            for (Map.Entry<Long, Bin.Header> entry : mapOfBinHeaders.entrySet())
            {
                if (entry.getValue().getNext() == 0L)
                {
                    entry.getValue().setNext(address);

                    ByteBuffer bytes = mutator.read(entry.getKey());

                    out = new ObjectOutputStream(new ByteBufferOutputStream(bytes));
                    out.writeObject(entry.getValue());
                    out.close();
                    
                    bytes.flip();
                    
                    mutator.write(entry.getKey(), bytes);
                    
                    break;
                }
            }
            
            schema = new Bin.Schema(this, strata, new HashMap<String, Index.Schema>());
            mapOfBinSchemas.put(klass, schema);
        }

        return schema;
    }
}