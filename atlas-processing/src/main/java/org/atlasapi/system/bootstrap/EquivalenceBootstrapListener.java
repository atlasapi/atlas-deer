package org.atlasapi.system.bootstrap;

import org.atlasapi.equivalence.EquivalenceRecord;
import org.atlasapi.equivalence.EquivalenceRecordStore;

import com.google.common.collect.ImmutableList;

public class EquivalenceBootstrapListener extends AbstractMultiThreadedBootstrapListener<EquivalenceRecord> {

    private final EquivalenceRecordStore equivStore;

    public EquivalenceBootstrapListener(int concurrencyLevel,
            EquivalenceRecordStore equivStore) {
        super(concurrencyLevel);
        this.equivStore = equivStore;
    }

    @Override
    protected void onChange(EquivalenceRecord change) {
        equivStore.writeRecords(ImmutableList.of(change));
    }

    
}
