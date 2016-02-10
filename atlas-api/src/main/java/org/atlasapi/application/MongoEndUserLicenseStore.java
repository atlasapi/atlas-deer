package org.atlasapi.application;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.mongodb.DBCollection;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

public class MongoEndUserLicenseStore implements EndUserLicenseStore {

    public static final String END_USER_LICENSE_COLLECTION = "endUserLicenses";

    private final EndUserLicenseTranslator translator = new EndUserLicenseTranslator();

    private final DBCollection endUserLicenses;

    public MongoEndUserLicenseStore(DatabasedMongo mongo) {
        this.endUserLicenses = mongo.collection(END_USER_LICENSE_COLLECTION);
    }

    @Override
    public EndUserLicense getById(Id id) {
        return translator.fromDBObject(endUserLicenses.findOne(where().idEquals(id.longValue())
                .build()));
    }

    @Override
    public void store(EndUserLicense license) {
        endUserLicenses.save(translator.toDBObject(license));
    }

}
