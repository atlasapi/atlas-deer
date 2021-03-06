package org.atlasapi.application;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.applications.client.metric.Metrics;
import com.metabroadcast.applications.client.model.internal.AccessRoles;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.applications.client.model.internal.Environment;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.media.entity.Publisher;

import java.time.ZonedDateTime;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultApplication {

    private static final String APP_CLIENT_ENV = checkNotNull(Configurer.get("applications.client.env").get());

    public static Application create() {

        ApplicationConfiguration configuration = ApplicationConfiguration.builder()
                .withPrecedence(getDefaultPublishers())
                .withEnabledWriteSources(ImmutableList.of())
                .build();

        return Application.builder()
                .withId(-1L)
                .withTitle("defaultApplication")
                .withDescription("Default application")
                .withEnvironment(Environment.parse(APP_CLIENT_ENV))
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
