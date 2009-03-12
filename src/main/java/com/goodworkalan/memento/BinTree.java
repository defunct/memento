package com.goodworkalan.memento;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.fossil.FossilStorage;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.ExtractorComparableFactory;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Schema;

// TODO Document.
public class BinTree
{
    // TODO Document.
    public Query<BinRecord> create(Mutator mutator)
    {
        Schema<BinRecord> schema = new Schema<BinRecord>();

        // FIXME Base on page size.
        schema.setLeafCapacity(256);
        schema.setInnerCapacity(256);
        schema.setComparableFactory(new ExtractorComparableFactory<BinRecord, Long>(new BinExtractor()));

        Stash stash = Fossil.newStash(mutator);
        FossilStorage<BinRecord> fossilStorage = new FossilStorage<BinRecord>(new BinRecordIO());
        long rootAddress = schema.create(stash, fossilStorage);
        
        return schema.open(stash, rootAddress, fossilStorage).query();
    }
}