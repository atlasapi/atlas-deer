package org.atlasapi.system.bootstrap;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.ElasticSearchContentIndexModule;
import org.atlasapi.messaging.MessagingModule;
import org.atlasapi.system.MetricsModule;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.bootstrap.workers.BootstrapWorkersModule;
import org.atlasapi.system.bootstrap.workers.DelegatingContentStore;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.legacy.MongoProgressStore;
import org.atlasapi.system.legacy.ProgressStore;

import com.metabroadcast.common.properties.Configurer;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@SuppressWarnings("PublicConstructor")
@Configuration
@Import({
        AtlasPersistenceModule.class,
        BootstrapWorkersModule.class,
        ProcessingHealthModule.class
})
public class BootstrapModule {

    // We only need 2 here, one to run the bootstrap and one to be able to return quickly
    // when it's running
    private static final Integer NUMBER_OF_SCHECHULE_CONTROLLER_THREADS = 2;
    private static final Integer NUMBER_OF_SCHEDULE_BOOTSTRAP_THREADS = Configurer.get(
            "boootstrap.schedule.numThreads").toInt();
    private static final Integer NUMBER_OF_SOURCE_BOOTSTRAP_TRHEADS = Configurer.get(
            "boootstrap.source.numThreads").toInt();

    @Autowired private AtlasPersistenceModule persistence;
    @Autowired private BootstrapWorkersModule workers;
    @Autowired private ElasticSearchContentIndexModule search;
    @Autowired private MetricRegistry metrics;
    @Autowired private DirectAndExplicitEquivalenceMigrator explicitEquivalenceMigrator;

    @Autowired private MessagingModule messaging;
    @Autowired private MetricsModule metricsModule;


    @Bean
    BootstrapController bootstrapController() {
        BootstrapController bootstrapController = new BootstrapController();

        bootstrapController.addBootstrapPair("legacy-content",
                new ResourceBootstrapper<>(persistence.legacyContentLister()),
                concurrencyLevel -> new ContentWritingBootstrapListener(
                        concurrencyLevel,
                        persistence.contentStore()
                )
        );

        bootstrapController.addBootstrapPair("legacy-topics",
                new ResourceBootstrapper<>(persistence.legacyTopicLister()),
                concurrencyLevel -> new TopicWritingBootstrapListener(
                        concurrencyLevel,
                        persistence.topicStore()
                )
        );

        return bootstrapController;
    }

    @Bean
    ContentBootstrapController contentBootstrapController() {
        return ContentBootstrapController.builder()
                .withRead(persistence.legacyContentResolver())
                .withContentLister(persistence.legacyContentLister())
                .withWrite(persistence.nullMessageSendingContentStore())
                .withContentIndex(search.equivContentIndex())
                .withEquivalenceMigrator(explicitEquivalenceMigrator)
                .withMaxSourceBootstrapThreads(NUMBER_OF_SOURCE_BOOTSTRAP_TRHEADS)
                .withProgressStore(progressStore())
                .withMetrics(metrics)
                .withEquivalentContentStore(persistence.nullMessageSendingEquivalentContentStore())
                .withEquivalenceGraphStore(persistence.nullMessageSendingEquivalenceGraphStore())
                .withContentStore(persistence.contentStore())
                .withNeo4JContentStore(persistence.neo4jContentStore())
                .build();
    }

    @Bean
    public ProgressStore progressStore() {
        return new MongoProgressStore(persistence.databasedWriteMongo());
    }

    @Bean
    IndividualTopicBootstrapController topicBootstrapController() {
        return new IndividualTopicBootstrapController(
                persistence.legacyTopicResolver(),
                persistence.topicStore()
        );
    }

    @Bean
    CqlContentBootstrapController cqlContentBootstrapController() {
        return CqlContentBootstrapController.create(
                executorService(20, "cql-content-bootstrap"),
                progressStore(),
                persistence.legacyContentResolver(),
                persistence.bootstrapCqlContentStore(),
                persistence.legacyContentLister(),
                metrics
        );
    }

    @Bean
    ChannelIntervalScheduleBootstrapTaskFactory scheduleBootstrapTaskFactory() {
        return new ChannelIntervalScheduleBootstrapTaskFactory(persistence.legacyScheduleStore(),
                persistence.scheduleStore(),
                new DelegatingContentStore(
                        persistence.legacyContentResolver(),
                        persistence.contentStore()
                )
        );
    }

    @Bean
    public ScheduleBootstrapController scheduleBootstrapController() {
        return new ScheduleBootstrapController(
                persistence.channelResolver(),
                executorService(
                        NUMBER_OF_SCHECHULE_CONTROLLER_THREADS,
                        "ScheduleBootstrapController"
                ),
                scheduleBootstrapper()
        );
    }

    @Bean
    public ScheduleBootstrapper scheduleBootstrapper() {
        return new ScheduleBootstrapper(
                executorService(NUMBER_OF_SCHEDULE_BOOTSTRAP_THREADS, "ScheduleBootstrapper"),
                workers.scheduleBootstrapTaskFactory(),
                workers.scheduleBootstrapWithContentMigrationTaskFactory(),
                workers.equivalenceWritingChannelIntervalScheduleBootstrapTaskFactory()
        );
    }

    @Bean
    public OrganisationBoostrapController organisationBootstrapController() {
        return new OrganisationBoostrapController(
                persistence.legacyOrganisationResolver(),
                persistence.idSettingOrganisationStore()
        );
    }

    private ListeningExecutorService executorService(Integer concurrencyLevel, String namePrefix) {
        return MoreExecutors.listeningDecorator(
                new ThreadPoolExecutor(
                        concurrencyLevel,
                        concurrencyLevel,
                        0, TimeUnit.MICROSECONDS,
                        new ArrayBlockingQueue<>(100 * Runtime.getRuntime().availableProcessors()),
                        new ThreadFactoryBuilder().setNameFormat(namePrefix + " Thread %d").build(),
                        new ThreadPoolExecutor.CallerRunsPolicy()
                )
        );
    }
}
