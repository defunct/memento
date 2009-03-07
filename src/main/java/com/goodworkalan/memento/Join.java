package com.goodworkalan.memento;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.goodworkalan.fossil.Fossil;
import com.goodworkalan.fossil.FossilStorage;
import com.goodworkalan.pack.Mutator;
import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Construction;
import com.goodworkalan.strata.Schema;

public final class Join
{
    private final Storage storage;
    
    private final Snapshot snapshot;

    private final JoinSchema joinSchema;

    private final Mutator mutator;

    private final Map<Link, Construction<JoinRecord, KeyList, Long>> isolation;

    public Join(Storage storage, Snapshot snapshot, Mutator mutator, JoinSchema joinSchema, List<Janitor> janitors)
    {
        Map<Link, Construction<JoinRecord, KeyList, Long>> isolation = new HashMap<Link, Construction<JoinRecord, KeyList, Long>>();

        for (JoinIndex index : joinSchema)
        {
            Schema<JoinRecord, KeyList> schema = Fossil.newFossilSchema();
            schema.setExtractor(new JoinExtractor());
            schema.setFieldCaching(true);
            
            Link link = index.getLink();
            Construction<JoinRecord, KeyList, Long> newStrata = schema.create(Fossil.initialize(new Stash(), mutator), new FossilStorage<JoinRecord, KeyList>(new JoinRecordIO(link.size())));
            
            isolation.put(link, newStrata);

            JoinJanitor janitor = new JoinJanitor(storage, link, newStrata);
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
        for (Construction<JoinRecord, KeyList, Long> query : isolation.values())
        {
            query.getQuery().flush();
        }
    }
    
    public void commit()
    {
    }
}