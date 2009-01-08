package com.goodworkalan.memento;

import java.io.Serializable;

import com.agtrz.depot.FieldExtractor;
import com.goodworkalan.strata.Schema;
import com.goodworkalan.strata.Strata;
import com.goodworkalan.strata.Tree;

public class IndexSchema
implements Serializable
{
    private static final long serialVersionUID = 20070610L;

    public final FieldExtractor extractor;

    public final Object strata;

    public final boolean unique;

    public final boolean notNull;

    public final Unmarshaller unmarshaller;

    public IndexSchema(Strata strata, FieldExtractor extractor, boolean unique, boolean notNull, Unmarshaller unmarshaller)
    {
        this.extractor = extractor;
        this.strata = strata;
        this.unique = unique;
        this.notNull = notNull;
        this.unmarshaller = unmarshaller;
    }

    private IndexSchema(Schema<IndexRecord, IndexTransaction> strata, FieldExtractor extractor, boolean unique, boolean notNull, Unmarshaller unmarshaller)
    {
        this.extractor = extractor;
        this.strata = strata;
        this.unique = unique;
        this.notNull = notNull;
        this.unmarshaller = unmarshaller;
    }

    public Strata getStrata()
    {
        return (Strata) strata;
    }

    public Tree<IndexRecord, IndexTransaction> toStrata(Object txn)
    {
        return new Schema<IndexRecord, IndexTransaction>(((Schema) strata).newStrata(txn), extractor, unique, notNull, unmarshaller);
    }

    public Schema<IndexRecord, IndexTransaction> toSchema()
    {
        return new Schema(getStrata().getSchema(), extractor, unique, notNull, unmarshaller);
    }
}
