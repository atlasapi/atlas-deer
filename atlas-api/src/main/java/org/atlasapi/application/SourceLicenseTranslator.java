package org.atlasapi.application;

import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class SourceLicenseTranslator {

    public static final String SOURCE_KEY = MongoConstants.ID;
    public static final String LICENSE_KEY = "license";

    public DBObject toDBObject(SourceLicense sourceLicense) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, SOURCE_KEY, sourceLicense.getSource().key());
        TranslatorUtils.from(dbo, LICENSE_KEY, sourceLicense.getLicense());
        return dbo;
    }

    public SourceLicense fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        return SourceLicense.builder()
                .withSource(Publisher.fromKey(TranslatorUtils.toString(dbo, SOURCE_KEY))
                        .requireValue())
                .withLicense(TranslatorUtils.toString(dbo, LICENSE_KEY))
                .build();
    }
}
