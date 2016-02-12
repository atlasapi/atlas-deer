package org.atlasapi.system.bootstrap.workers;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.ElasticSearchContentIndexModule;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.event.EventResolver;
import org.atlasapi.event.EventWriter;
import org.atlasapi.eventV2.EventV2;
import org.atlasapi.eventV2.EventV2Resolver;
import org.atlasapi.eventV2.EventV2Writer;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.messaging.v3.JacksonMessageSerializer;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.ProcessingMetricsModule;
import org.atlasapi.system.bootstrap.ChannelIntervalScheduleBootstrapTaskFactory;
import org.atlasapi.system.bootstrap.EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory;
import org.atlasapi.system.bootstrap.ScheduleBootstrapWithContentMigrationTaskFactory;
import org.atlasapi.system.legacy.LegacyPersistenceModule;
import org.atlasapi.topic.TopicResolver;
import org.atlasapi.topic.TopicStore;

import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;
import com.metabroadcast.common.queue.kafka.KafkaMessageConsumerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import org.joda.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

@Configuration
@Import({
        AtlasPersistenceModule.class, KafkaMessagingModule.class, LegacyPersistenceModule.class,
        ProcessingHealthModule.class })
public class BootstrapWorkersModule {

    private String consumerSystem = Configurer.get("messaging.system").get();
    private String zookeeper = Configurer.get("messaging.zookeeper").get();
    private String originSystem = Configurer.get("messaging.bootstrap.system").get();

    private Integer consumers = Configurer.get("messaging.bootstrap.consumers.default").toInt();
    private Integer maxConsumers = Configurer.get("messaging.bootstrap.consumers.max").toInt();

    private String contentChanges = Configurer.get("messaging.destination.content.changes").get();
    private String topicChanges = Configurer.get("messaging.destination.topics.changes").get();
    private String scheduleChanges = Configurer.get("messaging.destination.schedule.changes").get();
    private String eventChanges = Configurer.get("messaging.destination.event.changes").get();
    private String organisationChanges = Configurer.get("messaging.destination.organisation.changes")
            .get();

    private Duration backOffBase = Duration.millis(Configurer.get("messaging.maxBackOffMillis")
            .toLong());
    private Duration maxBackOff = Duration.millis(Configurer.get("messaging.maxBackOffMillis")
            .toLong());

    private Boolean v2ScheduleEnabled = Configurer.get("schedule.v2.enabled").toBoolean();

    private Boolean contentBootstrapEnabled = Configurer.get("messaging.enabled.content.bootstrap")
            .toBoolean();
    private Boolean scheduleBootstrapEnabled = Configurer.get("messaging.enabled.schedule.bootstrap")
            .toBoolean();
    private Boolean topicBootstrapEnabled = Configurer.get("messaging.enabled.topic.bootstrap")
            .toBoolean();
    private Boolean eventBoostrapEnabled = Configurer.get("messaging.enabled.event.bootstrap")
            .toBoolean();
    private Boolean organisationBootstrapEnabled = Configurer.get(
            "messaging.enabled.organisation.bootstrap").toBoolean();

    private Set<Publisher> ignoredScheduleSources
            = Sets.difference(
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
        return new KafkaMessageConsumerFactory(zookeeper, originSystem, backOffBase, maxBackOff);
    }

    @Bean
    @Lazy(true)
    KafkaConsumer contentBootstrapWorker() {
        ContentResolver legacyResolver = legacy.legacyContentResolver();
        ContentBootstrapWorker worker = new ContentBootstrapWorker(
                legacyResolver,
                persistence.contentStore(),
                metricsModule.metrics().timer("ContentBootstrapWorker")
        );
        MessageSerializer<ResourceUpdatedMessage> serializer =
                new EntityUpdatedLegacyMessageSerializer();
        return bootstrapQueueFactory().createConsumer(
                worker,
                serializer,
                contentChanges,
                "ContentBootstrap"
        )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    KafkaConsumer organisationBootstrapWorker() {
        OrganisationBootstrapWorker worker = new OrganisationBootstrapWorker(
                legacy.legacyOrganisationResolver(),
                persistence.idSettingOrganisationStore(),
                metricsModule.metrics().timer("OrganisationBootstrapWorker")
        );
        MessageSerializer<ResourceUpdatedMessage> serializer =
                new EntityUpdatedLegacyMessageSerializer();
        return bootstrapQueueFactory().createConsumer(
                worker,
                serializer,
                organisationChanges,
                "OrganisationBootstrap"
        )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    KafkaConsumer scheduleReadWriter() {
        ScheduleReadWriteWorker worker = new ScheduleReadWriteWorker(
                scheduleBootstrapTaskFactory(),
                persistence.channelResolver(),
                ignoredScheduleSources,
                metricsModule.metrics().timer("ScheduleBootstrapWorker")
        );
        MessageSerializer<ScheduleUpdateMessage> serializer
                = JacksonMessageSerializer.forType(ScheduleUpdateMessage.class);
        return bootstrapQueueFactory().createConsumer(
                worker,
                serializer,
                scheduleChanges,
                "ScheduleBootstrap"
        )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    KafkaConsumer scheduleV2ReadWriter() {
        ScheduleReadWriteWorker worker = new ScheduleReadWriteWorker(
                scheduleV2BootstrapTaskFactory(),
                persistence.channelResolver(),
                ignoredScheduleSources,
                metricsModule.metrics().timer("ScheduleV2BootstrapWorker")
        );
        MessageSerializer<ScheduleUpdateMessage> serializer
                = JacksonMessageSerializer.forType(ScheduleUpdateMessage.class);
        return bootstrapQueueFactory().createConsumer(
                worker,
                serializer,
                scheduleChanges,
                "ScheduleBootstrapV2"
        )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    KafkaConsumer topicReadWriter() {
        TopicResolver legacyResolver = legacy.legacyTopicResolver();
        TopicStore writer = persistence.topicStore();
        TopicReadWriteWorker worker = new TopicReadWriteWorker(
                legacyResolver,
                writer,
                metricsModule.metrics().timer("TopicBootstrapWorker")
        );
        MessageSerializer<ResourceUpdatedMessage> serializer =
                new EntityUpdatedLegacyMessageSerializer();
        return bootstrapQueueFactory().createConsumer(
                worker,
                serializer,
                topicChanges,
                "TopicBootstrap"
        )
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    KafkaConsumer eventReadWriter() {
        EventResolver legacyResolver = legacy.legacyEventResolver();
        EventWriter writer = persistence.eventWriter();
        EventReadWriteWorker worker = new EventReadWriteWorker(
                legacyResolver,
                writer,
                metricsModule.metrics().timer("EventBootstrapWorker")
        );
        MessageSerializer<ResourceUpdatedMessage> serializer =
                new EntityUpdatedLegacyMessageSerializer();
        return bootstrapQueueFactory()
                .createConsumer(worker, serializer, eventChanges, "EventBootstrap")
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    KafkaConsumer separatingEventReadWriter() {
        EventV2Resolver legacyResolver = legacy.legacyEventV2Resolver();
        EventV2Writer writer = persistence.eventV2Writer();
        SeparatingEventReadWriteWorker worker = new SeparatingEventReadWriteWorker(
                legacyResolver,
                writer,
                metricsModule.metrics().timer("SeparatingEventBootstrapWorker")
        );
        MessageSerializer<ResourceUpdatedMessage> serializer =
                new EntityUpdatedLegacyMessageSerializer();
        return bootstrapQueueFactory()
                .createConsumer(worker, serializer, eventChanges, "SeparatingEventBootstrap")
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
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
        if (eventBoostrapEnabled) {
            services.add(eventReadWriter());
            services.add(separatingEventReadWriter());
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
        return new ChannelIntervalScheduleBootstrapTaskFactory(legacy.legacyScheduleStore(),
                persistence.scheduleStore(),
                new DelegatingContentStore(
                        legacy.legacyContentResolver(),
                        persistence.contentStore()
                )
        );
    }

    //    (ScheduleResolver scheduleResolver,
    //            ScheduleWriter scheduleWriter, ContentStore contentStore, ContentIndex contentIndex,
    //            DirectAndExplicitEquivalenceMigrator equivalenceMigrator, AtlasPersistenceModule persistence)

    @Bean
    public ScheduleBootstrapWithContentMigrationTaskFactory scheduleBootstrapWithContentMigrationTaskFactory() {
        return new ScheduleBootstrapWithContentMigrationTaskFactory(legacy.legacyScheduleStore(),
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
    // yes, I know.
    public EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory equivalenceWritingChannelIntervalScheduleBootstrapTaskFactory() {
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
