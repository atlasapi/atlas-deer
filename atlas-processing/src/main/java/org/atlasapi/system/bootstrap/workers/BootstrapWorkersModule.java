package org.atlasapi.system.bootstrap.workers;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.ElasticSearchContentIndexModule;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.messaging.v3.JacksonMessageSerializer;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.ProcessingMetricsModule;
import org.atlasapi.system.bootstrap.ChannelIntervalScheduleBootstrapTaskFactory;
import org.atlasapi.system.bootstrap.EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory;
import org.atlasapi.system.bootstrap.ScheduleBootstrapWithContentMigrationTaskFactory;
import org.atlasapi.system.legacy.LegacyPersistenceModule;

import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;
import com.metabroadcast.common.queue.kafka.KafkaMessageConsumerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

@Configuration
@Import({
        AtlasPersistenceModule.class,
        KafkaMessagingModule.class,
        LegacyPersistenceModule.class,
        ProcessingHealthModule.class
})
public class BootstrapWorkersModule {

    private final String consumerSystem = Configurer.get("messaging.system").get();
    private final String zookeeper = Configurer.get("messaging.zookeeper").get();
    private final String originSystem = Configurer.get("messaging.bootstrap.system").get();

    private final Integer contentChangesNumOfConsumers =
            Configurer.get("messaging.bootstrap.content.changes.consumers").toInt();
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
    private final Boolean scheduleBootstrapEnabled =
            Configurer.get("messaging.bootstrap.schedule.changes.enabled").toBoolean();
    private final Boolean topicBootstrapEnabled =
            Configurer.get("messaging.bootstrap.topics.changes.enabled").toBoolean();
    private final Boolean eventBootstrapEnabled =
            Configurer.get("messaging.bootstrap.event.changes.enabled").toBoolean();
    private final Boolean organisationBootstrapEnabled =
            Configurer.get("messaging.bootstrap.organisation.changes.enabled").toBoolean();

    private final Set<Publisher> ignoredScheduleSources = Sets.difference(
            Publisher.all(),
            ImmutableSet.of(Publisher.PA, Publisher.BBC_NITRO, Publisher.BT_BLACKOUT)
    );

    @Autowired
    private AtlasPersistenceModule persistence;
    @Autowired
    private LegacyPersistenceModule legacy;
    @Autowired
    private KafkaMessagingModule messaging;
    @Autowired
    private ProcessingMetricsModule metricsModule;
    @Autowired
    private ElasticSearchContentIndexModule search;

    private ServiceManager consumerManager;

    @Bean
    @Qualifier("bootstrap")
    KafkaMessageConsumerFactory bootstrapQueueFactory() {
        return KafkaMessageConsumerFactory.create(zookeeper, originSystem);
    }

    @Bean
    @Lazy
    KafkaConsumer contentBootstrapWorker() {
        ContentBootstrapWorker worker = new ContentBootstrapWorker(
                legacy.legacyContentResolver(),
                persistence.contentStore(),
                metricsModule.metrics().timer("ContentBootstrapWorker")
        );
        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        new EntityUpdatedLegacyMessageSerializer(),
                        contentChanges,
                        "ContentBootstrap"
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(contentChangesNumOfConsumers)
                .withMaxConsumers(contentChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy
    KafkaConsumer organisationBootstrapWorker() {
        OrganisationBootstrapWorker worker = new OrganisationBootstrapWorker(
                legacy.legacyOrganisationResolver(),
                persistence.idSettingOrganisationStore(),
                metricsModule.metrics().timer("OrganisationBootstrapWorker")
        );
        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        new EntityUpdatedLegacyMessageSerializer(),
                        organisationChanges,
                        "OrganisationBootstrap"
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(organisationChangesNumOfConsumers)
                .withMaxConsumers(organisationChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy
    KafkaConsumer scheduleReadWriter() {
        ScheduleReadWriteWorker worker = new ScheduleReadWriteWorker(
                scheduleBootstrapTaskFactory(),
                persistence.channelResolver(),
                ignoredScheduleSources,
                metricsModule.metrics().timer("ScheduleBootstrapWorker")
        );
        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        JacksonMessageSerializer.forType(ScheduleUpdateMessage.class),
                        scheduleChanges,
                        "ScheduleBootstrap"
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(scheduleChangesNumOfConsumers)
                .withMaxConsumers(scheduleChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy
    KafkaConsumer scheduleV2ReadWriter() {
        ScheduleReadWriteWorker worker = new ScheduleReadWriteWorker(
                scheduleV2BootstrapTaskFactory(),
                persistence.channelResolver(),
                ignoredScheduleSources,
                metricsModule.metrics().timer("ScheduleV2BootstrapWorker")
        );
        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        JacksonMessageSerializer.forType(ScheduleUpdateMessage.class),
                        scheduleChanges,
                        "ScheduleBootstrapV2"
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(scheduleChangesNumOfConsumers)
                .withMaxConsumers(scheduleChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy
    KafkaConsumer topicReadWriter() {
        TopicReadWriteWorker worker = new TopicReadWriteWorker(
                legacy.legacyTopicResolver(),
                persistence.topicStore(),
                metricsModule.metrics().timer("TopicBootstrapWorker")
        );
        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        new EntityUpdatedLegacyMessageSerializer(),
                        topicChanges,
                        "TopicBootstrap"
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(topicChangesNumOfConsumers)
                .withMaxConsumers(topicChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy
    KafkaConsumer eventReadWriter() {
        SeparatingEventReadWriteWorker worker = new SeparatingEventReadWriteWorker(
                legacy.legacyEventResolver(),
                persistence.eventWriter(),
                metricsModule.metrics().timer("SeparatingEventBootstrapWorker")
        );
        return bootstrapQueueFactory()
                .createConsumer(
                        worker,
                        new EntityUpdatedLegacyMessageSerializer(),
                        eventChanges,
                        "SeparatingEventBootstrap"
                )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(eventChangesNumOfConsumers)
                .withMaxConsumers(eventChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    public DirectAndExplicitEquivalenceMigrator explicitEquivalenceMigrator() {
        return new DirectAndExplicitEquivalenceMigrator(
                legacy.legacyContentResolver(),
                legacy.legacyEquivalenceStore(),
                persistence.nullMessageSendingGraphStore()
        );
    }

    @PostConstruct
    public void start() throws TimeoutException {
        ImmutableList.Builder<Service> services = ImmutableList.builder();

        if (contentBootstrapEnabled) {
            services.add(contentBootstrapWorker());
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
                legacy.legacyScheduleStore(),
                persistence.scheduleStore(),
                new DelegatingContentStore(
                        legacy.legacyContentResolver(),
                        persistence.contentStore()
                )
        );
    }

    @Bean
    public ScheduleBootstrapWithContentMigrationTaskFactory
    scheduleBootstrapWithContentMigrationTaskFactory() {
        return new ScheduleBootstrapWithContentMigrationTaskFactory(
                legacy.legacyScheduleStore(),
                persistence.scheduleStore(),
                new DelegatingContentStore(
                        legacy.legacyContentResolver(),
                        persistence.contentStore()
                ),
                search.equivContentIndex(),
                explicitEquivalenceMigrator(),
                persistence,
                legacy
        );
    }

    @Bean
    public EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory
    equivalenceWritingChannelIntervalScheduleBootstrapTaskFactory() {
        return new EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory(
                legacy.legacyScheduleStore(),
                persistence.scheduleStore(),
                new DelegatingContentStore(
                        legacy.legacyContentResolver(),
                        persistence.contentStore()
                ),
                persistence.getEquivalentScheduleStore(),
                persistence.getContentEquivalenceGraphStore()
        );
    }

    @Bean
    public ChannelIntervalScheduleBootstrapTaskFactory scheduleV2BootstrapTaskFactory() {
        return new ChannelIntervalScheduleBootstrapTaskFactory(legacy.legacyScheduleStore(),
                persistence.v2ScheduleStore(),
                new DelegatingContentStore(
                        legacy.legacyContentResolver(),
                        persistence.contentStore()
                )
        );
    }
}
