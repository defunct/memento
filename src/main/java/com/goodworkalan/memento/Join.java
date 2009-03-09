package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.fossil.FossilStorage;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.ExtractorComparableFactory;
import com.goodworkalan.strata.Schema;
import com.goodworkalan.strata.Strata;

public final class Join
{
    private final Storage storage;
    
    private final Snapshot snapshot;

    private final JoinSchema joinSchema;

    private final Mutator mutator;

    private final Map<Link, Strata<JoinRecord>> isolation;

    public Join(Storage storage, Snapshot snapshot, Mutator mutator, JoinSchema joinSchema, List<Janitor> janitors)
    {
        Map<Link, Strata<JoinRecord>> isolation = new HashMap<Link, Strata<JoinRecord>>();

        for (JoinIndex index : joinSchema)
        {
            Schema<JoinRecord> schema = new Schema<JoinRecord>();

            schema.setInnerCapacity(7);
            schema.setLeafCapacity(7);
            schema.setComparableFactory(new ExtractorComparableFactory<JoinRecord, KeyList>(new JoinExtractor()));
            
            Link link = index.getLink();
            FossilStorage<JoinRecord> fossilStorage = new FossilStorage<JoinRecord>(new JoinRecordIO(link.size()));

            Stash stash = Fossil.newStash(mutator);
            long rootAddress = schema.create(stash, fossilStorage);

            Strata<JoinRecord> strata = schema.open(stash, rootAddress, fossilStorage);
            
            isolation.put(link, strata);

            JoinJanitor janitor = new JoinJanitor(storage, link, strata);
            janitors.add(janitor);
        }

        // TODO I used to write out the janitor here. Why was that necessary?

        this.storage = storage;
        this.snapshot = snapshot;
        this.isolation = isolation;
        this.joinSchema = joinSchema;
        this.mutator = mutator;
    }

    void flush()
    {
        storage.getClass();
        snapshot.getClass();
        joinSchema.getClass();
        mutator.getClass();
        for (Strata<JoinRecord> strata : isolation.values())
        {
            strata.query(Fossil.newStash(mutator)).destroy();
        }
    }
    
    public void commit()
    {
    }
}