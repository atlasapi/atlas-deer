package org.atlasapi.legacy.application;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.atlasapi.application.v3.ApplicationAccessRole;
import org.atlasapi.application.v3.SourceStatus;
import org.atlasapi.application.v3.SourceStatus.SourceState;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ApplicationConfigurationTranslator {

    public static final String STATE_KEY = "state";
    public static final String PUBLISHER_KEY = "publisher";
    public static final String SOURCES_KEY = "sources";
    public static final String PRECEDENCE_KEY = "precedence";
    public static final String WRITABLE_KEY = "writable";
    public static final String CONTENT_HIERARCHY_PRECEDENCE = "contentHierarchyPrecedence";
    public static final String IMAGE_PRECEDENCE_ENABLED_KEY = "imagePrecedenceEnabled";
    public static final String ACCESS_ROLES = "accessRoles";

    public DBObject toDBObject(ApplicationConfiguration configuration) {
        BasicDBObject dbo = new BasicDBObject();

        TranslatorUtils.from(
                dbo,
                SOURCES_KEY,
                sourceStatusesToList(configuration.sourceStatuses())
        );
        TranslatorUtils.from(
                dbo,
                IMAGE_PRECEDENCE_ENABLED_KEY,
                configuration.imagePrecedenceEnabledRawValue()
        );

        if (configuration.precedenceEnabled()) {
            TranslatorUtils.fromList(
                    dbo,
                    Lists.transform(configuration.precedence(), Publisher.TO_KEY),
                    PRECEDENCE_KEY
            );
        } else {
            dbo.put(PRECEDENCE_KEY, null);
        }

        if (configuration.contentHierarchyPrecedence().isPresent()) {
            dbo.put(
                    CONTENT_HIERARCHY_PRECEDENCE,
                    Lists.transform(
                            configuration.contentHierarchyPrecedence().get(),
                            Publisher.TO_KEY
                    )
            );
        }

        dbo.put(
                WRITABLE_KEY,
                Lists.transform(
                        configuration.writableSources().asList(),
                        Publisher.TO_KEY
                )
        );

        TranslatorUtils.fromList(
                dbo,
                configuration.getAccessRoles()
                        .stream()
                        .map(ApplicationAccessRole::getRole)
                        .collect(MoreCollectors.toImmutableList()),
                ACCESS_ROLES
        );

        return dbo;
    }

    public ApplicationConfiguration fromDBObject(DBObject dbo) {
        List<DBObject> statusDbos = TranslatorUtils.toDBObjectList(dbo, SOURCES_KEY);
        Map<Publisher, SourceStatus> sourceStatuses = sourceStatusesFrom(statusDbos);

        Boolean imagePrecedenceEnabled = TranslatorUtils.toBoolean(
                dbo,
                IMAGE_PRECEDENCE_ENABLED_KEY
        );
        List<Publisher> precedence = sourcePrecedenceFrom(dbo);

        List<String> writableKeys = TranslatorUtils.toList(dbo, WRITABLE_KEY);
        Iterable<Publisher> writableSources = Lists.transform(writableKeys, Publisher.FROM_KEY);

        Optional<List<Publisher>> contentHierarchyPrecedenceSources = Optional.absent();
        if (dbo.containsField(CONTENT_HIERARCHY_PRECEDENCE)) {
            List<String> contentHierarchyPrecedenceKeys = TranslatorUtils.toList(
                    dbo,
                    CONTENT_HIERARCHY_PRECEDENCE
            );
            contentHierarchyPrecedenceSources = Optional.of(Lists.transform(
                    contentHierarchyPrecedenceKeys,
                    Publisher.FROM_KEY
            ));
        }

        ImmutableSet<ApplicationAccessRole> accessRoles = ImmutableSet.of();
        if (dbo.containsField(ACCESS_ROLES)) {
            accessRoles = TranslatorUtils.toList(
                    dbo,
                    ACCESS_ROLES
            )
                    .stream()
                    .map(ApplicationAccessRole::from)
                    .collect(MoreCollectors.toImmutableSet());
        }

        return ApplicationConfiguration.builder()
                .withSourceStatuses(sourceStatuses)
                .withPrecedence(precedence)
                .withWritableSources(writableSources)
                .withImagePrecedenceEnabled(imagePrecedenceEnabled)
                .withContentHierarchyPrecedence(contentHierarchyPrecedenceSources)
                .withAccessRoles(accessRoles)
                .build();
    }

    private BasicDBList sourceStatusesToList(Map<Publisher, SourceStatus> sourceStatuses) {
        BasicDBList statuses = new BasicDBList();
        for (Entry<Publisher, SourceStatus> sourceStatus : sourceStatuses.entrySet()) {
            statuses.add(new BasicDBObject(ImmutableMap.of(
                    PUBLISHER_KEY, sourceStatus.getKey().key(),
                    STATE_KEY, sourceStatus.getValue().getState().toString().toLowerCase(),
                    "enabled", sourceStatus.getValue().isEnabled()
            )));
        }
        return statuses;
    }

    private List<Publisher> sourcePrecedenceFrom(DBObject dbo) {
        if (dbo.get(PRECEDENCE_KEY) == null) {
            return null;
        }
        List<String> sourceKeys = TranslatorUtils.toList(dbo, PRECEDENCE_KEY);
        return Lists.transform(sourceKeys, Publisher.FROM_KEY);
    }

    private Map<Publisher, SourceStatus> sourceStatusesFrom(List<DBObject> list) {
        Builder<Publisher, SourceStatus> builder = ImmutableMap.builder();
        for (DBObject dbo : list) {
            builder.put(
                    Publisher.fromKey(TranslatorUtils.toString(dbo, PUBLISHER_KEY)).requireValue(),
                    sourceStatusFrom(dbo)
            );
        }
        return builder.build();
    }

    private SourceStatus sourceStatusFrom(DBObject dbo) {
        if (TranslatorUtils.toBoolean(dbo, "enabled")) {
            return SourceStatus.AVAILABLE_ENABLED;
        }
        switch (SourceState.valueOf(TranslatorUtils.toString(dbo, STATE_KEY).toUpperCase())) {
        case AVAILABLE:
            return SourceStatus.AVAILABLE_DISABLED;
        case REQUESTED:
            return SourceStatus.REQUESTED;
        case REVOKED:
            return SourceStatus.REVOKED;
        case ENABLEABLE:
            return SourceStatus.ENABLEABLE;
        default:
            return SourceStatus.UNAVAILABLE;
        }
    }
}