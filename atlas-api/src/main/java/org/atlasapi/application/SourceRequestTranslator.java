package org.atlasapi.application;

import org.atlasapi.entity.Id;
import org.atlasapi.source.Sources;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class SourceRequestTranslator {

    public static final String APPID_KEY = "appId";
    public static final String SOURCE_KEY = "publisher";
    public static final String USAGE_TYPE_KEY = "usageType";
    public static final String EMAIL_KEY = "email";
    public static final String APPURL_KEY = "appUrl";
    public static final String REASON_KEY = "reason";
    public static final String APPROVED_KEY = "approved";
    public static final String REQUESTED_AT_KEY = "requestedAt";
    public static final String APPROVED_AT_KEY = "approvedAt";
    public static final String LICENSE_ACCEPTED = "licenseAccepted";

    public DBObject toDBObject(SourceRequest sourceRequest) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, MongoConstants.ID, sourceRequest.getId().longValue());
        TranslatorUtils.from(dbo, APPID_KEY, sourceRequest.getAppId().longValue());
        TranslatorUtils.from(dbo, SOURCE_KEY, sourceRequest.getSource().key());
        TranslatorUtils.from(dbo, USAGE_TYPE_KEY, sourceRequest.getUsageType().toString());
        TranslatorUtils.from(dbo, EMAIL_KEY, sourceRequest.getEmail());
        TranslatorUtils.from(dbo, APPURL_KEY, sourceRequest.getAppUrl());
        TranslatorUtils.from(dbo, REASON_KEY, sourceRequest.getReason());
        TranslatorUtils.from(dbo, APPROVED_KEY, sourceRequest.isApproved());
        TranslatorUtils.fromDateTime(dbo, REQUESTED_AT_KEY, sourceRequest.getRequestedAt());
        if (sourceRequest.getApprovedAt().isPresent()) {
            TranslatorUtils.fromDateTime(dbo, APPROVED_AT_KEY, sourceRequest.getApprovedAt().get());
        }
        TranslatorUtils.from(dbo, LICENSE_ACCEPTED, sourceRequest.isLicenseAccepted());
        return dbo;
    }

    public SourceRequest fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        return SourceRequest.builder()
                .withId(Id.valueOf(TranslatorUtils.toLong(dbo, MongoConstants.ID)))
                .withAppId(Id.valueOf(TranslatorUtils.toLong(dbo, APPID_KEY)))
                .withSource(Sources.fromPossibleKey(TranslatorUtils.toString(dbo, SOURCE_KEY))
                        .get())
                .withUsageType(UsageType.valueOf(TranslatorUtils.toString(dbo, USAGE_TYPE_KEY)))
                .withEmail(TranslatorUtils.toString(dbo, EMAIL_KEY))
                .withAppUrl(TranslatorUtils.toString(dbo, APPURL_KEY))
                .withReason(TranslatorUtils.toString(dbo, REASON_KEY))
                .withApproved(TranslatorUtils.toBoolean(dbo, APPROVED_KEY))
                .withRequestedAt(TranslatorUtils.toDateTime(dbo, REQUESTED_AT_KEY))
                .withApprovedAt(TranslatorUtils.toDateTime(dbo, APPROVED_AT_KEY))
                .withLicenseAccepted(Boolean.TRUE.equals(TranslatorUtils.toBoolean(
                        dbo,
                        LICENSE_ACCEPTED
                )))
                .build();
    }
}
