package org.atlasapi.model.translators;

import java.util.List;
import java.util.Map;

import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationAccessRole;
import org.atlasapi.application.ApplicationCredentials;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.legacy.application.ApplicationConfiguration;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ApplicationModelTranslator
        implements Function<org.atlasapi.legacy.application.Application, Application> {

    public Iterable<Application> transform(
            Iterable<org.atlasapi.legacy.application.Application> inputs) {
        return Iterables.transform(inputs, this);
    }

    public Application apply(org.atlasapi.legacy.application.Application input) {
        return Application.builder()
                .withId(Id.valueOf(input.getDeerId().longValue()))
                .withSlug(input.getSlug())
                .withTitle(input.getTitle())
                .withDescription(input.getDescription())
                .withCreated(input.getCreated())
                .withCredentials(transformCredentials3to4(input.getCredentials()))
                .withSources(transformConfiguration3to4(input.getConfiguration()))
                .withRevoked(input.isRevoked())
                .build();
    }

    private ApplicationCredentials transformCredentials3to4(
            org.atlasapi.legacy.application.ApplicationCredentials input) {
        return new ApplicationCredentials(input.getApiKey());
    }

    private ApplicationSources transformConfiguration3to4(ApplicationConfiguration input) {
        List<SourceReadEntry> reads;
        if (input.precedenceEnabled()) {
            reads = asOrderedList(input.sourceStatuses(), input.precedence());
        } else {
            reads = asOrderedList(input.sourceStatuses(), input.sourceStatuses().keySet());
        }
        return ApplicationSources.builder()
                .withPrecedence(input.precedenceEnabled())
                .withReadableSources(reads)
                .withImagePrecedenceEnabled(input.imagePrecedenceEnabled())
                .withWritableSources(input.writableSources().asList())
                .withContentHierarchyPrecedence(input.contentHierarchyPrecedence().orNull())
                .withAccessRoles(input.getAccessRoles()
                        .stream()
                        .map(role -> ApplicationAccessRole.from(role.getRole()))
                        .collect(MoreCollectors.toImmutableSet())
                )
                .build()
                .copyWithMissingSourcesPopulated();
    }

    private List<SourceReadEntry> asOrderedList(
            Map<Publisher, org.atlasapi.application.v3.SourceStatus> readsMap,
            Iterable<Publisher> order) {
        ImmutableList.Builder<SourceReadEntry> builder = ImmutableList.builder();
        for (Publisher source : order) {
            builder.add(new SourceReadEntry(
                            source,
                            SourceStatusModelTranslator.transform3To4(readsMap.get(source))
                    )
            );
        }
        return builder.build();
    }

    public org.atlasapi.legacy.application.Application transform4to3(Application input) {
        return org.atlasapi.legacy.application.Application.application(input.getSlug())
                .withDeerId(input.getId().longValue())
                .withTitle(input.getTitle())
                .withDescription(input.getDescription())
                .createdAt(input.getCreated())
                .withConfiguration(transformSources4to3(input.getSources()))
                .withCredentials(transformCredentials4to3(input.getCredentials()))
                .withRevoked(input.isRevoked())
                .build();
    }

    private org.atlasapi.legacy.application.ApplicationCredentials transformCredentials4to3(
            ApplicationCredentials input) {
        return new org.atlasapi.legacy.application.ApplicationCredentials(input.getApiKey());
    }

    private ApplicationConfiguration transformSources4to3(ApplicationSources input) {
        Map<Publisher, org.atlasapi.application.v3.SourceStatus> sourceStatuses =
                readsAsMap(input.getReads());

        ApplicationConfiguration configuration = ApplicationConfiguration.defaultConfiguration()
                .withSources(sourceStatuses);

        if (input.isPrecedenceEnabled()) {
            List<Publisher> precedence = Lists.transform(
                    input.getReads(),
                    SourceReadEntry::getPublisher
            );
            configuration = configuration.copyWithPrecedence(precedence);
        }

        configuration = configuration.copyWithWritableSources(
                input.getWrites()
        );
        configuration = configuration.copyWithContentHierarchyPrecedence(
                input.contentHierarchyPrecedence().orNull()
        );
        configuration = configuration.copyWithImagePrecedenceEnabled(
                input.imagePrecedenceEnabled()
        );
        configuration = configuration.copyWithAccessRoles(
                input.getAccessRoles()
                        .stream()
                        .map(role -> org.atlasapi.application.v3.ApplicationAccessRole
                                .from(role.getRole()))
                        .collect(MoreCollectors.toImmutableSet())
        );

        return configuration;
    }

    private Map<Publisher, org.atlasapi.application.v3.SourceStatus> readsAsMap(
            List<SourceReadEntry> input) {
        Map<Publisher, org.atlasapi.application.v3.SourceStatus> sourceStatuses = Maps.newHashMap();
        for (SourceReadEntry entry : input) {
            sourceStatuses.put(
                    entry.getPublisher(),
                    SourceStatusModelTranslator.transform4To3(entry.getSourceStatus())
            );
        }
        return sourceStatuses;
    }
}
