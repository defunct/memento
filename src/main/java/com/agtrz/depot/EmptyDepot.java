package com.agtrz.depot;

import static com.agtrz.depot.Depot.COMMITTED;
import static com.agtrz.depot.Depot.HEADER_URI;
import static com.agtrz.depot.Depot.SIZEOF_INT;
import static com.agtrz.depot.Depot.SIZEOF_LONG;

import java.io.File;

import com.goodworkalan.memento.Snapshot;
import com.goodworkalan.pack.Pack;

final class EmptyDepot
{
    public final File file;

    public final Pack pack;

    public final Strata snapshots;

    public EmptyDepot(File file)
    {
        Pack.Creator newPack = new Pack.Creator();
        newPack.addStaticPage(Depot.HEADER_URI, Pack.ADDRESS_SIZE);
        Pack pack = newPack.create(file);
        Pack.Mutator mutator = pack.mutate();

        Fossil.Schema newMutationStorage = new Fossil.Schema();

        newMutationStorage.setWriter(new Snapshot.Writer());
        newMutationStorage.setReader(new Snapshot.Reader());
        newMutationStorage.setSize(Depot.SIZEOF_LONG + Depot.SIZEOF_INT);

        Strata.Schema newMutationStrata = new Strata.Schema();

        newMutationStrata.setStorage(newMutationStorage);
        newMutationStrata.setFieldExtractor(new Snapshot.Extractor());
        newMutationStrata.setSize(256);

        Object txn = Fossil.txn(mutator);
        Strata mutations = newMutationStrata.newStrata(txn);

        Strata.Query query = mutations.query(txn);
        query.insert(new Snapshot.Record(new Long(1L), Depot.COMMITTED));

        mutator.commit();

        this.file = file;
        this.pack = pack;
        this.snapshots = mutations;
    }
}