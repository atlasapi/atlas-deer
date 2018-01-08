package org.atlasapi.system.bootstrap.workers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.metabroadcast.columbus.telescope.client.TelescopeClientImpl;
import com.metabroadcast.columbus.telescope.client.TelescopeReporterFactory;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;
import com.metabroadcast.common.queue.kafka.KafkaMessageConsumerFactory;
import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.ElasticSearchContentIndexModule;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.EquivalentScheduleStoreScheduleUpdateWorker;
import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.messaging.v3.JacksonMessageSerializer;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.ProcessingMetricsModule;
import org.atlasapi.system.bootstrap.ChannelIntervalScheduleBootstrapTaskFactory;
import org.atlasapi.system.bootstrap.ColumbusTelescopeReporter;
import org.atlasapi.system.bootstrap.EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory;
import org.atlasapi.system.bootstrap.ScheduleBootstrapWithContentMigrationTaskFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Configuration
@Import({
        AtlasPersistenceModule.class,
        KafkaMessagingModule.class,
        ProcessingHealthModule.class
})
public class BootstrapWorkersModule {

    private static final String WORKER_METRIC_PREFIX = "messaging.worker.";

    private final String consumerSystem = Configurer.get("messaging.system").get();
    private final String zookeeper = Configurer.get("messaging.zookeeper").get();
    private final String originSystem = Configurer.get("messaging.bootstrap.system").get();

    private final Integer contentChangesNumOfConsumers =
            Configurer.get("messaging.bootstrap.content.changes.consumers").toInt();
    private final int cqlContentChangesNumOfConsumers =
            Configurer.get("messaging.bootstrap.cql-content.changes.consumers").toInt();
    private final Integer topicChangesNumOfConsumers =
            Configurer.get("messaging.bootstrap.topics.changes.consumers").toInt();
    private final Integer scheduleChangesNumOfConsumers =
            Configurer.get("messaging.bootstrap.schedule.changes.consumers").toInt();
    private final Integer eventChangesNumOfConsumers =
            Configurer.get("messaging.bootstrap.event.changes.consumers").toInt();
    private final Integer organisationChangesNumOfConsumers =
            Configurer.get("messaging.bootstrap.organisation.changes.consumers").toInt();

    private final String contentChanges =
            Configurer.get("messaging.destination.content.changes").get();
    private final String topicChanges =
            Configurer.get("messaging.destination.topics.changes").get();
    private final String scheduleChanges =
            Configurer.get("messaging.destination.schedule.changes").get();
    private final String eventChanges =
            Configurer.get("messaging.destination.event.changes").get();
    private final String organisationChanges =
            Configurer.get("messaging.destination.organisation.changes").get();

    private final Boolean v2ScheduleEnabled = Configurer.get("schedule.v2.enabled").toBoolean();
    private final Boolean contentBootstrapEnabled =
            Configurer.get("messaging.bootstrap.content.changes.enabled").toBoolean();
    private final Boolean cqlContentBootstrapEnabled =
            Configurer.get("messaging.bootstrap.cql-content.changes.enabled").toBoolean();
    private final Boolean scheduleBootstrapEnabled =
            Configurer.get("messaging.bootstrap.schedule.changes.enabled").toBoolean();
    private final Boolean topicBootstrapEnabled =
            Configurer.get("messaging.bootstrap.topics.changes.enabled").toBoolean();
    private final Boolean eventBootstrapEnabled =
            Configurer.get("messaging.bootstrap.event.changes.enabled").toBoolean();
    private final Boolean organisationBootstrapEnabled =
            Configurer.get("messaging.bootstrap.organisation.changes.enabled").toBoolean();

    private final String columbusTelescopeHost =
            Configurer.get("reporting.columbus-telescope.host").get();
    private final String reportingEnvironment = Configurer.getPlatform();
    private final Integer columbusTelescopeThreadCapacity =
            Configurer.get("reporting.columbus-telescope.thread.capacity").toInt();
    private final Integer columbusTelescopeThreadPoolSize =
            Configurer.get("reporting.columbus-telescope.thread.pool.size").toInt();
    private final Integer columbusTelescopeThreadPoolSizeMax =
            Configurer.get("reporting.columbus-telescope.thread.pool.size.max").toInt();

    private final Set<Publisher> ignoredScheduleSources = Sets.difference(
            Publisher.all(),
            ImmutableSet.of(Publisher.PA, Publisher.BBC_NITRO, Publisher.BT_BLACKOUT)
    );

    @Autowired
    private AtlasPersistenceModule persistence;
    @Autowired
    private KafkaMessagingModule messaging;
    @Autowired
    private ProcessingMetricsModule metricsModule;
    @Autowired
    private ElasticSearchContentIndexModule search;

    private ServiceManager consumerManager;

    @Bean
    @Qualifier("bootstrap")
    public KafkaMessageConsumerFactory bootstrapQueueFactory() {
        return KafkaMessageConsumerFactory.create(zookeeper, originSystem);
    }

    @Bean
    @Lazy
    public KafkaConsumer contentBootstrapWorker() {
        String workerName = "ContentBootstrap";

        ContentBootstrapWorker worker = ContentBootstrapWorker.create(
                persistence.legacyContentResolver(),
                persistence.astyanaxContentStore(),
                WORKER_METRIC_PREFIX + workerName + ".",
                metricsModule.metrics(),
                ColumbusTelescopeReporter.create(
                        TelescopeClientImpl.create(
                                columbusTelescopeHost,
                                getTelescopeExecutor(),
                                null,
                                null
                        ),
                        reportingEnvironment,
                        getObjectMapper()
                )
        );

        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        new EntityUpdatedLegacyMessageSerializer(),
                        contentChanges,
                        workerName
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(contentChangesNumOfConsumers)
                .withMaxConsumers(contentChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    public KafkaConsumer cqlContentBootstrapWorker() {
        String workerName = "CqlContentBootstrap";

        ContentBootstrapWorker worker = ContentBootstrapWorker.create(
                persistence.legacyContentResolver(),
                persistence.contentStore(),
                WORKER_METRIC_PREFIX + workerName + ".",
                metricsModule.metrics(),
                ColumbusTelescopeReporter.create(
                        TelescopeClientImpl.create(
                                columbusTelescopeHost,
                                getTelescopeExecutor(),
                                null,
                                null
                        ),
                        reportingEnvironment,
                        getObjectMapper()
                )
        );

        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        new EntityUpdatedLegacyMessageSerializer(),
                        contentChanges,
                        workerName
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(cqlContentChangesNumOfConsumers)
                .withMaxConsumers(cqlContentChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    KafkaConsumer organisationBootstrapWorker() {
        String workerName = "OrganisationBootstrap";

        OrganisationBootstrapWorker worker = OrganisationBootstrapWorker.create(
                persistence.legacyOrganisationResolver(),
                persistence.idSettingOrganisationStore(),
                WORKER_METRIC_PREFIX + workerName + ".",
                metricsModule.metrics()
        );

        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        new EntityUpdatedLegacyMessageSerializer(),
                        organisationChanges,
                        workerName
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(organisationChangesNumOfConsumers)
                .withMaxConsumers(organisationChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    KafkaConsumer scheduleReadWriter() {
        String workerName = "ScheduleBootstrap";

        ScheduleReadWriteWorker worker = ScheduleReadWriteWorker.create(
                scheduleBootstrapTaskFactory(),
                persistence.channelResolver(),
                ignoredScheduleSources,
                WORKER_METRIC_PREFIX + workerName + ".",
                metricsModule.metrics()
        );

        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        JacksonMessageSerializer.forType(ScheduleUpdateMessage.class),
                        scheduleChanges,
                        workerName
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(scheduleChangesNumOfConsumers)
                .withMaxConsumers(scheduleChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    KafkaConsumer scheduleV2ReadWriter() {
        String workerName = "ScheduleBootstrapV2";

        ChannelIntervalScheduleBootstrapTaskFactory scheduleV2BootstrapTaskFactory =
                new ChannelIntervalScheduleBootstrapTaskFactory(
                        persistence.legacyScheduleStore(),
                        persistence.v2ScheduleStore(),
                        new DelegatingContentStore(
                                persistence.legacyContentResolver(),
                                persistence.contentStore()
                        )
                );

        ScheduleReadWriteWorker worker = ScheduleReadWriteWorker.create(
                scheduleV2BootstrapTaskFactory,
                persistence.channelResolver(),
                ignoredScheduleSources,
                WORKER_METRIC_PREFIX + workerName + ".",
                metricsModule.metrics()
        );

        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        JacksonMessageSerializer.forType(ScheduleUpdateMessage.class),
                        scheduleChanges,
                        workerName
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(scheduleChangesNumOfConsumers)
                .withMaxConsumers(scheduleChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    KafkaConsumer topicReadWriter() {
        String workerName = "TopicBootstrap";

        TopicReadWriteWorker worker = TopicReadWriteWorker.create(
                persistence.legacyTopicResolver(),
                persistence.topicStore(),
                WORKER_METRIC_PREFIX + workerName + ".",
                metricsModule.metrics()
        );

        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        new EntityUpdatedLegacyMessageSerializer(),
                        topicChanges,
                        workerName
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(topicChangesNumOfConsumers)
                .withMaxConsumers(topicChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    KafkaConsumer eventReadWriter() {
        String workerName = "SeparatingEventBootstrap";

        SeparatingEventReadWriteWorker worker = SeparatingEventReadWriteWorker.create(
                persistence.legacyEventResolver(),
                persistence.eventWriter(),
                WORKER_METRIC_PREFIX + workerName + ".",
                metricsModule.metrics()
        );

        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        new EntityUpdatedLegacyMessageSerializer(),
                        eventChanges,
                        workerName
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(eventChangesNumOfConsumers)
                .withMaxConsumers(eventChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @PostConstruct
    public void start() throws TimeoutException {
        ImmutableList.Builder<Service> services = ImmutableList.builder();

        if (contentBootstrapEnabled) {
            services.add(contentBootstrapWorker());
        }
        if (cqlContentBootstrapEnabled) {
            services.add(cqlContentBootstrapWorker());
        }
        if (scheduleBootstrapEnabled) {
            services.add(scheduleReadWriter());
        }
        if (v2ScheduleEnabled) {
            services.add(scheduleV2ReadWriter());
        }
        if (topicBootstrapEnabled) {
            services.add(topicReadWriter());
        }
        if (eventBootstrapEnabled) {
            services.add(eventReadWriter());
        }
        if (organisationBootstrapEnabled) {
            services.add(organisationBootstrapWorker());
        }

        consumerManager = new ServiceManager(services.build());
        consumerManager.startAsync().awaitHealthy(1, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void stop() throws TimeoutException {
        consumerManager.stopAsync().awaitStopped(1, TimeUnit.MINUTES);
    }

    @Bean
    public ChannelIntervalScheduleBootstrapTaskFactory scheduleBootstrapTaskFactory() {
        return new ChannelIntervalScheduleBootstrapTaskFactory(
                persistence.legacyScheduleStore(),
                persistence.scheduleStore(),
                new DelegatingContentStore(
                        persistence.legacyContentResolver(),
                        persistence.contentStore()
                )
        );
    }

    @Bean
    public ScheduleBootstrapWithContentMigrationTaskFactory
    scheduleBootstrapWithContentMigrationTaskFactory() {
        return new ScheduleBootstrapWithContentMigrationTaskFactory(
                persistence.legacyScheduleStore(),
                persistence.scheduleStore(),
                new DelegatingContentStore(
                        persistence.legacyContentResolver(),
                        persistence.contentStore()
                ),
                search.equivContentIndex(),
                directAndExplicitEquivalenceMigrator(),
                persistence,
                persistence.legacySegmentMigrator(),
                persistence.legacyContentResolver()
        );
    }

    @Bean
    public EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory
    equivalenceWritingChannelIntervalScheduleBootstrapTaskFactory() {
        return new EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory(
                persistence.legacyScheduleStore(),
                persistence.scheduleStore(),
                new DelegatingContentStore(
                        persistence.legacyContentResolver(),
                        persistence.contentStore()
                ),
                persistence.getEquivalentScheduleStore(),
                persistence.getContentEquivalenceGraphStore()
        );
    }

    @Bean
    public ChannelIntervalScheduleBootstrapTaskFactory
    forwardingScheduleBootstrapTaskFactory() {
        return new ChannelIntervalScheduleBootstrapTaskFactory(
                persistence.legacyScheduleStore(),
                persistence.persistenceModule().getForwardingScheduleStore(ImmutableList.of(
                        EquivalentScheduleStoreScheduleUpdateWorker.create(
                                persistence.getEquivalentScheduleStore(),
                                WORKER_METRIC_PREFIX + "ForwardingScheduleStore.Schedule",
                                metricsModule.metrics()
                        )
                )),
                new DelegatingContentStore(
                        persistence.legacyContentResolver(),
                        persistence.contentStore()
                )
        );
    }

    @Bean
    public DirectAndExplicitEquivalenceMigrator directAndExplicitEquivalenceMigrator() {
        return DirectAndExplicitEquivalenceMigrator.create(
                persistence.legacyContentResolver(),
                persistence.legacyEquivalenceStore(),
                persistence.nullMessageSendingGraphStore()
        );
    }

    private ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.registerModule(new JodaModule());
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new JacksonMessageSerializer.MessagingModule());

        return mapper;
    }

    private ThreadPoolExecutor getTelescopeExecutor() {
        return new ThreadPoolExecutor(
                columbusTelescopeThreadPoolSize, columbusTelescopeThreadPoolSizeMax,
                500,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(columbusTelescopeThreadCapacity),
                new TelescopeReporterFactory.RejectedExecutionHandlerImpl()
        );
    }
}
