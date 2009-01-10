package com.goodworkalan.memento;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Query;
import com.goodworkalan.strata.Schema;

public class BinTree
{
    public Query<BinRecord, Long> create(Mutator mutator)
    {
        Schema<BinRecord, Long> newFossil;
        newFossil = Fossil.newFossilSchema(new BinRecordIO());

        // FIXME Base on page size.
        newFossil.setLeafSize(256);
        newFossil.setInnerSize(256);
        newFossil.setExtractor(new BinExtractor());
        newFossil.setFieldCaching(true);
        
//        newStrata.setMaxDirtyTiers(1);???

        return newFossil.newTransaction(Fossil.initialize(new Stash(), mutator));
    }
}