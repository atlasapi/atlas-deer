package org.atlasapi.application;

import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MongoSourceLicenseStoreTest {

    private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();

    @Test
    public void test() {
        SourceLicenseStore store = new MongoSourceLicenseStore(mongo);
        SourceLicense license = SourceLicense.builder()
                .withSource(Publisher.BBC)
                .withLicense("A license")
                .build();
        store.store(license);
        SourceLicense retrieved = store.licenseFor(Publisher.BBC).get();
        assertEquals(license.getSource(), retrieved.getSource());
        assertEquals(license.getLicense(), retrieved.getLicense());
    }

}
