package com.goodworkalan.memento;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.fossil.FossilStorage;
import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.ilk.Ilk.Key;
import com.goodworkalan.pack.Creator;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.pack.Opener;
import com.goodworkalan.pack.Pack;
import com.goodworkalan.pack.io.PackInputStream;
import com.goodworkalan.pack.io.PackOutputStream;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.ExtractorComparableFactory;
import com.goodworkalan.strata.Schema;
import com.goodworkalan.strata.Strata;

public class FileStorage implements PackFactory
{
    /** The URI of the header of the linked list of storage definitions. */
    private final static URI LOADER_HEADER_URI = URI.create("http://goodworkalan.com/page/Memento.html#loader");
    
    /** The URI of the address of the snapshot index. */
    private final static URI SNAPSHOT_HEADER_URI = URI.create("http://goodworkalan.com/page/Memento.html#snapshot");
    
    /** The pack file. */
    private final File file;
    
    /** The pack. */
    private Pack pack;
    
    /** A map of bin super type tokens to bin indexes. */
    private final ConcurrentMap<Ilk.Key, PackStrata<BinRecord>> binIndexes = new ConcurrentHashMap<Ilk.Key, PackStrata<BinRecord>>();
    
    /**
     * A map of super type tokens to the pack used to allocate blocks. In the
     * case of file storage, it is the same pack for every bin, but I'm thinking
     * ahead.
     */
    private final Map<Ilk.Key, PackFile> binHeaps = new HashMap<Ilk.Key, PackFile>();
    
    /** A map of super type tokens to the address of the bin definition. */
    private final Map<Ilk.Key, Long> binDefinitions = new HashMap<Ilk.Key, Long>();
    
    /**  The b-tree used to order snapshot records. */
    private PackStrata<SnapshotRecord> snapshots;

    /**
     * Create a file storage strategy that will store the entire database in a
     * single pack file.
     * 
     * @param file
     *            The file to to write the pack.
     */
    public FileStorage(File file)
    {
        this.file = file;
    }

    /**
     * Write the address of the snapshot index to the snapshot header in the
     * given pack file.
     * 
     * @param pack
     *            The pack.
     * @param address
     *            The address of the snapshot index.
     */
    private void writeSnapshot(Mutator mutator, long address)
    {
        ByteBuffer header = mutator.read(mutator.getPack().getStaticBlocks().get(SNAPSHOT_HEADER_URI));
        header.putLong(address);
        header.flip();
        mutator.write(mutator.getPack().getStaticBlocks().get(SNAPSHOT_HEADER_URI), header);
    }
    
    /**
     * Create a b-tree schema for snapshot records.
     * 
     * @return A new b-tree schema for snapshot records.
     */
    private Schema<SnapshotRecord> newSnapshotSchema()
    {
        Schema<SnapshotRecord> schema = new Schema<SnapshotRecord>();
        
        schema.setInnerCapacity(7);
        schema.setLeafCapacity(7);
        schema.setComparableFactory(new ExtractorComparableFactory<SnapshotRecord, Long>(new SnapshotExtractor()));
        
        return schema;
    }

    public void create()
    {
        if (file.exists())
        {
            throw new MementoException(MementoException.BOGUS_EXCEPTION_THROWN_BY_LOSER_BOY);
        }
        
        FileChannel fileChannel;
        try
        {
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
        }
        catch (FileNotFoundException e)
        {
            throw new MementoException(MementoException.FILE_NOT_FOUND_EXCEPTION, e);
        }
        
        Creator creator = new Creator();
        creator.addStaticBlock(LOADER_HEADER_URI, Long.SIZE / Byte.SIZE);
        Pack pack = creator.create(fileChannel);
    
        Mutator mutator = pack.mutate();
        
        ByteBuffer header = mutator.read(mutator.getPack().getStaticBlocks().get(LOADER_HEADER_URI));
        header.putLong(0);
        header.flip();
        mutator.write(mutator.getPack().getStaticBlocks().get(LOADER_HEADER_URI), header);
        
        Stash stash = Fossil.newStash(mutator);
        FossilStorage<SnapshotRecord> snapshotStorage = new FossilStorage<SnapshotRecord>(new SnapshotRecordIO());
        Schema<SnapshotRecord> snapshotSchema = newSnapshotSchema();
        
        long address  = snapshotSchema.create(stash, snapshotStorage);
        writeSnapshot(mutator, address);
        
        mutator.commit();
        
        pack.close();
    }

    /**
     * Open the file storage returning true if there is no need for recovery and
     * the file storage is ready to load.
     * 
     * @return True if the database shutdown soft, false if it shutdown hard.
     */
    public boolean open()
    {
        FileChannel fileChannel;
        try
        {
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
        }
        catch (FileNotFoundException e)
        {
            throw new MementoException(MementoException.FILE_NOT_FOUND_EXCEPTION, e);
        }
        Opener opener = new Opener();
        return opener.open(fileChannel);
    }
    
    // TODO Document.
    public void recover()
    {
    }

    /** Load the b-tree and heap information from disk after opening. */
    public void load()
    {
        Mutator mutator = pack.mutate();
        
        long snapshotAddress = mutator.read(pack.getStaticBlocks().get(SNAPSHOT_HEADER_URI)).getLong();
        snapshots = new PackStrata<SnapshotRecord>(file, pack, newSnapshotSchema().open(Fossil.newStash(mutator), snapshotAddress, new FossilStorage<SnapshotRecord>(new SnapshotRecordIO()))); 
        
        try
        {
            ByteBuffer header = mutator.read(mutator.getPack().getStaticBlocks().get(LOADER_HEADER_URI)); 
            long address = header.getLong();
            while (address != 0L)
            {
                ObjectInputStream in = new ObjectInputStream(new PackInputStream(mutator, address));
                LoaderNode loaderNode = (LoaderNode) in.readObject();
                in.close();
    
                loaderNode.getLoader().load(address, this);
                address = loaderNode.getNext();
            }        
        }
        catch (IOException e)
        {
            throw new MementoException(MementoException.BOGUS_EXCEPTION_THROWN_BY_LOSER_BOY, e);
        }
        catch (ClassNotFoundException e)
        {
            throw new MementoException(MementoException.BOGUS_EXCEPTION_THROWN_BY_LOSER_BOY, e);
        }
        finally
        {
            mutator.commit();
        }
    }

    /**
     * Get the b-tree used to order snapshot records.
     * 
     * @return The b-tree used to order snapshot records.
     */
    public PackStrata<SnapshotRecord> getSnapshots()
    {
        return snapshots;
    }
    
    /**
     * Write the given loader linking it into the linked list of bin definition
     * in the definition pack file.
     * 
     * @param loader
     *            The loader.
     * @return The address of the block where the definition is stored.
     */
    private synchronized long add(Loader loader)
    {
        try
        {
            Mutator mutator = pack.mutate();
        
            ByteBuffer header = mutator.read(mutator.getPack().getStaticBlocks().get(LOADER_HEADER_URI));
            long next = header.getLong(0);
            LoaderNode loaderNode = new LoaderNode(next, loader);
            
            PackOutputStream packOut = new PackOutputStream(mutator);
            ObjectOutputStream out = new ObjectOutputStream(packOut);
            out.writeObject(loaderNode);
            out.close();
            
            long address = packOut.allocate();
            
            header.putLong(address);
            header.flip();
            
            mutator.write(mutator.getPack().getStaticBlocks().get(LOADER_HEADER_URI), header);
            
            mutator.commit();
            
            return address;
        }
        catch (IOException e)
        {
            throw new MementoException(MementoException.BOGUS_EXCEPTION_THROWN_BY_LOSER_BOY, e);
        }
    }

    /**
     * Create a b-tree schema for bin records.
     * 
     * @return A new b-tree schema for bin records.
     */
    private Schema<BinRecord> newBinSchema()
    {
        Schema<BinRecord> schema = new Schema<BinRecord>();
        
        schema.setInnerCapacity(7);
        schema.setLeafCapacity(7);
        schema.setComparableFactory(new ExtractorComparableFactory<BinRecord, Long>(new BinExtractor()));
    
        return schema;
    }

    /**
     * Create storage for the bin that stores objects of the type indicated by
     * the given super type token.
     * 
     * @param bin
     *            The super type token of the bin.
     */
    public void create(Ilk.Key bin)
    {
        binHeaps.put(bin, new PackFile(file, pack));

        if (!binIndexes.containsKey(bin))
        {
            Mutator mutator = pack.mutate();
            Stash stash = Fossil.newStash(mutator);

            Schema<BinRecord> schema = new Schema<BinRecord>();
            
            schema.setInnerCapacity(7);
            schema.setLeafCapacity(7);
            schema.setComparableFactory(new ExtractorComparableFactory<BinRecord, Long>(new BinExtractor()));
            
            FossilStorage<BinRecord> storage = new FossilStorage<BinRecord>(new BinRecordIO());
            
            long address = schema.create(stash, storage);
            
            Strata<BinRecord> strata = schema.open(new Stash(), address, storage);
            
            PackStrata<BinRecord> packStrata = new PackStrata<BinRecord>(file, pack, strata);
            
            mutator.commit();

            if (binIndexes.putIfAbsent(bin, packStrata) == null)
            {
                destroy(packStrata);
            }
            else
            {
                binDefinitions.put(bin, add(new BinLoader(bin, file, file, address)));
            }
        }
    }

    /**
     * Get the b-tree used to locate object block addresses by key for the bin
     * that stores objects of the type indicated by the given super type token.
     * 
     * @param bin
     *            The super type token of the bin.
     * @return The b-tree used locate object block address by key.
     */
    public PackStrata<BinRecord> getBinIndex(Key bin)
    {
        return binIndexes.get(bin);
    }

    /**
     * Get the pack used to allocate blocks to store objects for the bin that
     * stores objects of the type indicated by the given super type token.
     * 
     * @param bin
     *            The super type token of the bin.
     * @return The pack used to allocate blcoks for the given bin.
     */
    public PackFile getBinHeap(Key bin)
    {
        return binHeaps.get(bin);
    }

    /**
     * Release all of the b-tree pages held by the given b-tree structure.
     * 
     * @param packStrata
     *            The b-tree structure.
     */
    private void destroy(PackStrata<?> packStrata)
    {
        Mutator mutator = pack.mutate();
        packStrata.getStrata().query(Fossil.newStash(mutator)).destroy();
        mutator.commit();
    }

    /**
     * Destroy the storage for the bin that stores objects of the type indicated
     * by the given super type token.
     * 
     * @param bin
     *            The super type token of the bin.
     */
    public void remove(Key bin)
    {
        binHeaps.remove(bin);
        PackStrata<BinRecord> packStrata = binIndexes.remove(bin);
        if (packStrata != null)
        {
            destroy(packStrata);
        }
    }

    // TODO Document.
    public void load(long address, Ilk.Key bin, File heapFile, File indexFile, long indexAddress)
    {
        Mutator mutator = pack.mutate();
        
        Schema<BinRecord> schema = newBinSchema();

        Strata<BinRecord> strata = schema.open(Fossil.newStash(mutator), indexAddress, new FossilStorage<BinRecord>(new BinRecordIO()));

        binIndexes.put(bin, new PackStrata<BinRecord>(file, pack, strata));
        binHeaps.put(bin, new PackFile(file, pack));
        binDefinitions.put(bin, address);

        mutator.commit();
    }
    
    // TODO Document.
    public void create(Key bin, Key index)
    {
    }

    // TODO Document.
    public PackStrata<IndexRecord> getIndex(Key bin, Key index)
    {
        return null;
    }

    // TODO Document.
    public void remove(Key bin, Key index)
    {
    }

    // TODO Document.
    public void create(Link link)
    {
    }

    // TODO Document.
    public PackStrata<JoinRecord> getJoin(Link link)
    {
        return null;
    }

    // TODO Document.
    public void remove(Link link)
    {
    }

    // TODO Document.
    public void close()
    {
    }
}
