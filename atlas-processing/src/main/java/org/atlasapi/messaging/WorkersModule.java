package org.atlasapi.messaging;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.schedule.ScheduleUpdateMessage;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.ProcessingMetricsModule;
import org.atlasapi.system.bootstrap.workers.ContentEquivalenceAssertionLegacyMessageSerializer;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.bootstrap.workers.LegacyRetryingContentResolver;

import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageConsumerBuilder;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

@Configuration
@Import({ AtlasPersistenceModule.class, KafkaMessagingModule.class, ProcessingHealthModule.class })
public class WorkersModule {

    private String consumerSystem = Configurer.get("messaging.system").get();

    private String contentChanges = Configurer.get("messaging.destination.content.changes").get();
    private String topicChanges = Configurer.get("messaging.destination.topics.changes").get();
    private String scheduleChanges = Configurer.get("messaging.destination.schedule.changes").get();
    private String contentEquivalenceGraphChanges = Configurer.get(
            "messaging.destination.equivalence.content.graph.changes").get();
    private String equivalentContentChanges = Configurer.get(
            "messaging.destination.equivalent.content.changes").get();

    private Integer defaultTopicIndexers = Configurer.get(
            "messaging.topic.indexing.consumers.default").toInt();
    private Integer maxTopicIndexers = Configurer.get("messaging.topic.indexing.consumers.max")
            .toInt();

    private Integer defaultContentIndexers = Configurer.get(
            "messaging.content.indexing.consumers.default").toInt();
    private Integer maxContentIndexers = Configurer.get("messaging.content.indexing.consumers.max")
            .toInt();

    private Integer equivDefaultConsumers = Configurer.get("equiv.update.consumers.default")
            .toInt();
    private Integer equivMaxConsumers = Configurer.get("equiv.update.consumers.max").toInt();

    private String equivSystem = Configurer.get("equiv.update.producer.system").get();
    private String equivTopic = Configurer.get("equiv.update.producer.topic").get();

    private Boolean equivalentScheduleStoreContentUpdatesEnabled = Configurer.get(
            "equiv.schedule.content.updates.enabled").toBoolean();

    private Boolean contentIndexerEnabled = Configurer.get("messaging.enabled.content.indexer")
            .toBoolean();
    private Boolean equivalentContentStoreEnabled = Configurer.get(
            "messaging.enabled.content.equivalent.store").toBoolean();
    private Boolean equivalentContentGraphEnabled = Configurer.get(
            "messaging.enabled.content.equivalent.graph").toBoolean();
    private Boolean equivalentScheduleStoreEnabled = Configurer.get(
            "messaging.enabled.schedule.equivalent.store").toBoolean();
    private Boolean equivalentScheduleGraphEnabled = Configurer.get(
            "messaging.enabled.schedule.equivalent.graph").toBoolean();
    private Boolean topicIndexerEnabled = Configurer.get("messaging.enabled.topic.indexer")
            .toBoolean();
    private Boolean equivalenceGraphEnabled = Configurer.get("messaging.enabled.equivalence.graph")
            .toBoolean();

    @Autowired
    private KafkaMessagingModule messaging;
    @Autowired
    private AtlasPersistenceModule persistence;
    @Autowired
    private ProcessingMetricsModule metricsModule;
    private ServiceManager consumerManager;

    private <M extends Message> MessageSerializer<M> serializer(Class<M> cls) {
        return JacksonMessageSerializer.forType(cls);
    }

    @Bean
    @Lazy(true)
    public Worker<ResourceUpdatedMessage> topicIndexingWorker() {
        return new TopicIndexingWorker(
                persistence.topicStore(),
                persistence.topicIndex(),
                metricsModule
                        .metrics()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer topicIndexerMessageListener() {
        return messaging.messageConsumerFactory().createConsumer(topicIndexingWorker(),
                serializer(ResourceUpdatedMessage.class), topicChanges, "TopicIndexer"
        )
                .withDefaultConsumers(defaultTopicIndexers)
                .withMaxConsumers(maxTopicIndexers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalenceGraphUpdateMessage> equivalentContentStoreGraphUpdateWorker() {
        return new EquivalentContentStoreGraphUpdateWorker(
                persistence.getEquivalentContentStore(),
                metricsModule
                        .metrics()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentContentStoreGraphUpdateListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(equivalentContentStoreGraphUpdateWorker(),
                        serializer(EquivalenceGraphUpdateMessage.class),
                        contentEquivalenceGraphChanges, "EquivalentContentStoreGraphs"
                )
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<ResourceUpdatedMessage> equivalentContentStoreContentUpdateWorker() {
        return new EquivalentContentStoreContentUpdateWorker(
                persistence.getEquivalentContentStore(),
                new LegacyRetryingContentResolver(
                        persistence.contentStore(),
                        persistence.legacyContentResolver(),
                        persistence.nullMessageSendingContentStore()
                ),
                metricsModule.metrics()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentContentStoreContentUpdateListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(equivalentContentStoreContentUpdateWorker(),
                        serializer(ResourceUpdatedMessage.class),
                        contentChanges, "EquivalentContentStoreContent"
                )
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalenceGraphUpdateMessage> equivalentScheduletStoreGraphUpdateWorker() {
        return new EquivalentScheduleStoreGraphUpdateWorker(
                persistence.getEquivalentScheduleStore(),
                metricsModule
                        .metrics()
        );
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalentContentUpdatedMessage> equivalentScheduletStoreContentUpdateWorker() {
        return new EquivalentScheduleStoreContentUpdateWorker(
                persistence.getEquivalentContentStore(),
                persistence.getEquivalentScheduleStore(),
                metricsModule.metrics()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentScheduleStoreGraphUpdateListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(equivalentScheduletStoreGraphUpdateWorker(),
                        serializer(EquivalenceGraphUpdateMessage.class),
                        contentEquivalenceGraphChanges, "EquivalentScheduleStoreGraphs"
                )
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentScheduleStoreContentListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(equivalentScheduletStoreContentUpdateWorker(),
                        serializer(EquivalentContentUpdatedMessage.class),
                        equivalentContentChanges, "EquivalentScheduleStoreContent"
                )
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<ScheduleUpdateMessage> equivalentScheduleStoreScheduleUpdateWorker() {
        return new EquivalentScheduleStoreScheduleUpdateWorker(
                persistence.getEquivalentScheduleStore(),
                metricsModule
                        .metrics()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentScheduleStoreScheduleUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(
                equivalentScheduleStoreScheduleUpdateWorker(),
                serializer(ScheduleUpdateMessage.class),
                scheduleChanges,
                "EquivalentScheduleStoreSchedule"
        )
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalenceAssertionMessage> contentEquivalenceUpdater() {
        return new ContentEquivalenceUpdatingWorker(
                persistence.getContentEquivalenceGraphStore(),
                metricsModule.metrics(),
                explicitEquivalenceMigrator()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(contentEquivalenceUpdater(),
                new ContentEquivalenceAssertionLegacyMessageSerializer(),
                equivTopic,
                "EquivGraphUpdate"
        )
                .withProducerSystem(equivSystem)
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public EquivalentContentIndexingContentWorker equivalentContentIndexingWorker() {
        return new EquivalentContentIndexingContentWorker(
                persistence.contentStore(), persistence.contentIndex(), metricsModule.metrics()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentContentIndexingMessageListener() {
        MessageConsumerBuilder<KafkaConsumer, EquivalentContentUpdatedMessage> consumer =
                messaging.messageConsumerFactory().createConsumer(
                        equivalentContentIndexingWorker(),
                        serializer(EquivalentContentUpdatedMessage.class),
                        equivalentContentChanges,
                        "EquivalentContentIndexer"
                );
        return consumer.withMaxConsumers(maxContentIndexers)
                .withDefaultConsumers(defaultContentIndexers)
                .withConsumerSystem(consumerSystem)
                .build();
    }

    @Bean
    @Lazy(true)
    public EquivalentContentIndexingGraphWorker equivalentContentIndexingGraphWorker() {
        return new EquivalentContentIndexingGraphWorker(
                persistence.contentIndex(),
                metricsModule.metrics()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentContentIndexingGraphMessageListener() {
        MessageConsumerBuilder<KafkaConsumer, EquivalenceGraphUpdateMessage> consumer =
                messaging.messageConsumerFactory().createConsumer(
                        equivalentContentIndexingGraphWorker(),
                        serializer(EquivalenceGraphUpdateMessage.class),
                        contentEquivalenceGraphChanges,
                        "EquivalentContentIndexer"
                );
        return consumer.withMaxConsumers(maxContentIndexers)
                .withDefaultConsumers(equivDefaultConsumers)
                .withConsumerSystem(consumerSystem)
                .build();
    }

    @PostConstruct
    public void start() throws TimeoutException {
        ImmutableList.Builder<Service> services = ImmutableList.builder();

        if (equivalentScheduleStoreContentUpdatesEnabled) {
            services.add(equivalentScheduleStoreContentListener());
        }
        if (contentIndexerEnabled) {
            services.add(equivalentContentIndexingMessageListener());
            services.add(equivalentContentIndexingGraphMessageListener());
        }
        if (equivalentContentStoreEnabled) {
            services.add(equivalentContentStoreContentUpdateListener());
        }
        if (equivalentContentGraphEnabled) {
            services.add(equivalentContentStoreGraphUpdateListener());
        }
        if (equivalentScheduleStoreEnabled) {
            services.add(equivalentScheduleStoreScheduleUpdateListener());
        }
        if (equivalentScheduleGraphEnabled) {
            services.add(equivalentScheduleStoreGraphUpdateListener());
        }
        if (topicIndexerEnabled) {
            services.add(topicIndexerMessageListener());
        }
        if (equivalenceGraphEnabled) {
            services.add(equivUpdateListener());
        }

        consumerManager = new ServiceManager(services.build());
        consumerManager.startAsync().awaitHealthy(1, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void stop() throws TimeoutException {
        consumerManager.stopAsync().awaitStopped(1, TimeUnit.MINUTES);
    }

    public DirectAndExplicitEquivalenceMigrator explicitEquivalenceMigrator() {
        return new DirectAndExplicitEquivalenceMigrator(
                persistence.legacyContentResolver(),
                persistence.legacyEquivalenceStore(),
                persistence.getContentEquivalenceGraphStore()
        );
    }
}