package org.atlasapi.application;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metabroadcast.applications.client.metric.Metrics;
import com.metabroadcast.applications.client.model.internal.AccessRoles;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.media.entity.Publisher;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultApplication {

    public static Application create() {
        return createInternal(Lists.newArrayList());
    }

    public static Application createWithReads(List<Publisher> publishers) {
        return createInternal(publishers);
    }

    private static Application createInternal(List<Publisher> publishers) {

        List<Publisher> finalPublishers = ImmutableList.<Publisher>builder()
                .addAll(getDefaultPublishers().stream()
                                .filter(publisher -> !publishers.contains(publisher))
                                .collect(Collectors.toList())
                )
                .addAll(publishers)
                .build();

        ApplicationConfiguration configuration = ApplicationConfiguration.builder()
                .withPrecedence(finalPublishers)
                .withEnabledWriteSources(ImmutableList.of())
                .build();

        return Application.builder()
                .withId(-1l)
                .withTitle("defaultApplication")
                .withDescription("Default application")
                .withEnvironment(null)
                .withCreated(ZonedDateTime.now())
                .withApiKey("default")
                .withSources(configuration)
                .withAllowedDomains(ImmutableList.of())
                .withAccessRoles(
                        AccessRoles.create(
                                ImmutableList.of(),
                                Metrics.create(new MetricRegistry())
                        )
                )
                .withRevoked(false)
                .build();
    }

    private static List<Publisher> getDefaultPublishers() {
        return Publisher.all()
                .stream()
                .filter(Publisher::enabledWithNoApiKey)
                .collect(MoreCollectors.toImmutableList());
    }

}
