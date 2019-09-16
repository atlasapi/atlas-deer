package org.atlasapi.system.bootstrap;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.kafka.StartPoint;
import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.ElasticSearchContentIndexModule;
import org.atlasapi.messaging.MessagingModule;
import org.atlasapi.system.MetricsModule;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.bootstrap.workers.BootstrapWorkersModule;
import org.atlasapi.system.bootstrap.workers.DelegatingContentStore;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.bootstrap.workers.EntityUpdatedLegacyMessageSerializer;
import org.atlasapi.system.legacy.MongoProgressStore;
import org.atlasapi.system.legacy.ProgressStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("PublicConstructor")
@Configuration
@Import({
        AtlasPersistenceModule.class,
        BootstrapWorkersModule.class,
        ProcessingHealthModule.class
})
public class BootstrapModule {

    private static final Integer NUMBER_OF_SCHEDULE_BOOTSTRAP_THREADS = Configurer.get(
            "bootstrap.schedule.numThreads").toInt();
    private static final Integer NUMBER_OF_SOURCE_BOOTSTRAP_THREADS = Configurer.get(
            "bootstrap.source.numThreads").toInt();

    private final Integer contentChangesReplayNumOfConsumers =
            Configurer.get("messaging.bootstrap.content.changes.consumers").toInt();
    private final String consumerSystem = Configurer.get("messaging.system").get();
    private final String contentChanges =
            Configurer.get("messaging.destination.content.changes").get();

    @Autowired private AtlasPersistenceModule persistence;
    @Autowired private BootstrapWorkersModule workers;
    @Autowired private ElasticSearchContentIndexModule search;
    @Autowired private MetricRegistry metrics;

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
                .withWrite(persistence.contentStore())
                .withNullMessageSenderWrite(persistence.nullMessageSendingContentStore())
                .withContentIndex(search.equivContentIndex())
                .withEquivalenceMigrator(
                        DirectAndExplicitEquivalenceMigrator.create(
                                persistence.legacyContentResolver(),
                                persistence.legacyEquivalenceStore(),
                                persistence.getContentEquivalenceGraphStore()
                        )
                )
                .withMaxSourceBootstrapThreads(NUMBER_OF_SOURCE_BOOTSTRAP_THREADS)
                .withProgressStore(progressStore())
                .withMetrics(metrics)
                .withEquivalentContentStore(persistence.getEquivalentContentStore())
                .withNullMessageSenderEquivalentContentStore(persistence.nullMessageSendingEquivalentContentStore())
                .withEquivalenceGraphStore(persistence.nullMessageSendingEquivalenceGraphStore())
                .withContentStore(persistence.contentStore())
                .withNeo4JContentStore(persistence.neo4jContentStore())
                .withLegacyResolver(persistence.legacyContentResolver())
                .withReplayConsumerFactory(worker -> workers.bootstrapQueueFactory()
                        .createConsumer(
                                worker,
                                new EntityUpdatedLegacyMessageSerializer(),
                                contentChanges,
                                "ReplayContentBootstrap"
                        )
                        .withConsumerSystem(consumerSystem)
                        .withDefaultConsumers(contentChangesReplayNumOfConsumers)
                        .withMaxConsumers(contentChangesReplayNumOfConsumers)
                        .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                        .withMetricRegistry(metricsModule.metrics())
                        .startFrom(StartPoint.BEGINNING)
                        .build()
                ).build();
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
    EventBootstrapController eventBootstrapController() {
        return new EventBootstrapController(persistence.eventResolver(), persistence.eventWriter());
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
                scheduleBootstrapper()
        );
    }

    @Bean
    public ScheduleBootstrapper scheduleBootstrapper() {
        return new ScheduleBootstrapper(
                executorService(NUMBER_OF_SCHEDULE_BOOTSTRAP_THREADS, "ScheduleBootstrapper"),
                workers.scheduleBootstrapTaskFactory(),
                workers.scheduleBootstrapWithContentMigrationTaskFactory(),
                workers.equivalenceWritingChannelIntervalScheduleBootstrapTaskFactory(),
                workers.forwardingScheduleBootstrapTaskFactory()
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
