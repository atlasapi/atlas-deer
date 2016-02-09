package org.atlasapi.application;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EndUserLicenseStoreTest {

    private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();

    @Test
    public void test() {
        final Id id = Id.valueOf(5000);
        EndUserLicenseStore store = new MongoEndUserLicenseStore(mongo);
        EndUserLicense license = EndUserLicense.builder()
                .withId(id)
                .withLicense("Test text")
                .build();
        store.store(license);
        EndUserLicense retrieved = store.getById(id);
        assertEquals(license.getId(), retrieved.getId());
        assertEquals(license.getLicense(), retrieved.getLicense());
    }

}
