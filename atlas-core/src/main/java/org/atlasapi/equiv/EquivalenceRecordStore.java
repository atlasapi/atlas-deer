package org.atlasapi.equiv;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.collect.OptionalMap;

public interface EquivalenceRecordStore {

    void writeRecords(Iterable<EquivalenceRecord> record);
    
    OptionalMap<Id, EquivalenceRecord> resolveRecords(Iterable<Id> ids);
    
}
