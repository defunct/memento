package com.agtrz.depot;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.strata.Transaction;

public class BinTree
{
    public Transaction<BinRecord, Mutator> create(Mutator mutator)
    {
        com.goodworkalan.strata.Schema<BinRecord, Mutator> newFossil;
        newFossil = Fossil.newFossilSchema(new BinRecordIO());

        // FIXME Base on page size.
        newFossil.setLeafSize(256);
        newFossil.setInnerSize(256);
        newFossil.setExtractor(new BinExtractor());
        newFossil.setFieldCaching(true);
        
//        newStrata.setMaxDirtyTiers(1);???

        return newFossil.newTransaction(mutator);
    }
}