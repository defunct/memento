package com.agtrz.depot;

import static com.agtrz.depot.Depot.COMMITTED;
import static com.agtrz.depot.Depot.HEADER_URI;
import static com.agtrz.depot.Depot.SIZEOF_INT;
import static com.agtrz.depot.Depot.SIZEOF_LONG;
import static com.agtrz.depot.Depot.SIZEOF_SHORT;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.goodworkalan.pack.Pack;

public final class Creator
    {
        private final Map<String, Bin.Creator> mapOfBinCreators = new HashMap<String, Bin.Creator>();

        private final Map<String, Join.Creator> mapOfJoinCreators = new HashMap<String, Join.Creator>();

        public Bin.Creator newBin(String name)
        {
            if (mapOfBinCreators.containsKey(name))
            {
                throw new IllegalStateException();
            }
            Bin.Creator newBin = new Bin.Creator(name);
            mapOfBinCreators.put(name, newBin);
            return newBin;
        }

        public Join.Creator newJoin(String name)
        {
            if (mapOfJoinCreators.containsKey(name))
            {
                throw new IllegalStateException();
            }
            Join.Creator newJoin = new Join.Creator(mapOfBinCreators.keySet());
            mapOfJoinCreators.put(name, newJoin);
            return newJoin;
        }

        public Depot create(File file)
        {
            return create(file, new NullSync());
        }

        public Depot create(File file, Sync sync)
        {
            Pack.Creator newBento = new Pack.Creator();
            newBento.addStaticPage(Depot.HEADER_URI, Pack.ADDRESS_SIZE);
            Pack bento = newBento.create(file);
            Pack.Mutator mutator = bento.mutate();

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

            Map<String, Bin.Schema> mapOfBins = new HashMap<String, Bin.Schema>();
            for (Map.Entry<String, Bin.Creator> entry : mapOfBinCreators.entrySet())
            {
                String name = (String) entry.getKey();

                Fossil.Schema newBinStorage = new Fossil.Schema();
                newBinStorage.setWriter(new Bin.Writer());
                newBinStorage.setReader(new Bin.Reader());
                newBinStorage.setSize(Depot.SIZEOF_LONG * 2 + Pack.ADDRESS_SIZE);

                Strata.Schema newBinStrata = new Strata.Schema();

                newBinStrata.setStorage(newBinStorage);
                newBinStrata.setFieldExtractor(new Bin.Extractor());
                newBinStrata.setSize(220);
                newBinStrata.setMaxDirtyTiers(1);

                Strata strata = newBinStrata.newStrata(Fossil.txn(mutator));

                Bin.Creator newBin = (Bin.Creator) entry.getValue();

                Map<String, Index.Schema> mapOfIndices = new HashMap<String, Index.Schema>();
                for (Map.Entry<String, Index.Creator> index : newBin.mapOfIndices.entrySet())
                {
                    String nameOfIndex = (String) index.getKey();

                    Fossil.Schema newIndexStorage = new Fossil.Schema();
                    newIndexStorage.setWriter(new Index.Writer());
                    newIndexStorage.setReader(new Index.Reader());
                    newIndexStorage.setSize(Depot.SIZEOF_LONG + Depot.SIZEOF_LONG + Depot.SIZEOF_SHORT);

                    Strata.Schema newIndexStrata = new Strata.Schema();
                    Index.Creator newIndex = (Index.Creator) index.getValue();

                    if (newIndex.unmarshaller == null)
                    {
                        newIndex.setUnmarshaller(newBin.unmarshaller);
                    }

                    newIndexStrata.setStorage(newIndexStorage);
                    newIndexStrata.setFieldExtractor(new Index.Extractor());
                    newIndexStrata.setSize(256);
                    newIndexStrata.setMaxDirtyTiers(1);
                    newIndexStrata.setCacheFields(true);

                    Strata indexStrata = newIndexStrata.newStrata(Fossil.txn(mutator));

                    mapOfIndices.put(nameOfIndex, new Index.Schema(indexStrata, newIndex.extractor, newIndex.unique, newIndex.notNull, newIndex.unmarshaller));
                }

//                mapOfBins.put(name, new Bin.Schema(strata, mapOfIndices, newBin.unmarshaller, newBin.marshaller));
            }

            Map<String, Join.Schema> mapOfJoins = new HashMap<String, Join.Schema>();
            for (Map.Entry<String, Join.Creator> join : mapOfJoinCreators.entrySet())
            {
                String joinName = (String) join.getKey();
                Join.Creator newJoin = (Join.Creator) join.getValue();

                Join.Index[] indexes = new Join.Index[newJoin.listOfAlternates.size() + 1];

                String[] order = (String[]) newJoin.mapOfFields.keySet().toArray(new String[newJoin.mapOfFields.size()]);
                indexes[0] = newJoinIndex(mutator, newJoin.mapOfFields, order);

                Map<String, String> mapOfFields = new LinkedHashMap<String, String>(newJoin.mapOfFields);
                for (int i = 0; i < newJoin.listOfAlternates.size(); i++)
                {
                    order = (String[]) newJoin.listOfAlternates.get(i);
                    indexes[i + 1] = newJoinIndex(mutator, mapOfFields, order);
                }
                mapOfJoins.put(joinName, new Join.Schema(indexes, mapOfFields));
            }

            PackOutputStream allocation = new PackOutputStream(mutator);
            try
            {
                ObjectOutputStream out = new ObjectOutputStream(allocation);
                out.writeObject(mutations);
                out.writeObject(mapOfBins);
                out.writeObject(mapOfJoins);
            }
            catch (IOException e)
            {
                throw new Danger("io", e, 0);
            }

            long addressOfBins = allocation.allocate();

            ByteBuffer block = mutator.read(mutator.getSchema().getStaticPageAddress(Depot.HEADER_URI));

            block.putLong(addressOfBins);
            block.flip();

            mutator.write(mutator.getSchema().getStaticPageAddress(Depot.HEADER_URI), block);
            
            mutator.commit();
            bento.close();

            return new Opener().open(file, sync);
        }
    }