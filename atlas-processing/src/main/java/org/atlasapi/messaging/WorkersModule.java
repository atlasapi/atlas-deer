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
@Import({
        AtlasPersistenceModule.class,
        KafkaMessagingModule.class,
        ProcessingHealthModule.class
})
public class WorkersModule {

    private final String consumerSystem = Configurer.get("messaging.system").get();

    private final String contentChanges =
            Configurer.get("messaging.destination.content.changes").get();
    private final String topicChanges =
            Configurer.get("messaging.destination.topics.changes").get();
    private final String scheduleChanges =
            Configurer.get("messaging.destination.schedule.changes").get();
    private final String contentEquivalenceGraphChanges =
            Configurer.get("messaging.destination.equivalence.content.graph.changes").get();
    private final String equivalentContentChanges =
            Configurer.get("messaging.destination.equivalent.content.changes").get();

    private final Integer topicIndexingNumOfConsumers =
            Configurer.get("messaging.deer.topic.indexer.consumers").toInt();
    private final Integer equivContentContentChangesNumOfConsumers =
            Configurer.get("messaging.deer.equivalent.content.content.changes.consumers").toInt();
    private final Integer equivContentGraphChangesNumOfConsumers =
            Configurer.get("messaging.deer.equivalent.content.graph.changes.consumers").toInt();
    private final Integer equivScheduleContentChangesNumOfConsumers =
            Configurer.get("messaging.deer.equivalent.schedule.content.changes.consumers").toInt();
    private final Integer equivScheduleGraphChangesNumOfConsumers =
            Configurer.get("messaging.deer.equivalent.schedule.graph.changes.consumers").toInt();
    private final Integer equivScheduleScheduleChangesNumOfConsumers =
            Configurer.get("messaging.deer.equivalent.schedule.schedule.changes.consumers").toInt();
    private final Integer contentEquivalenceGraphChangesNumOfConsumers =
            Configurer.get("messaging.deer.equivalence.content.graph.changes.consumers").toInt();
    private final Integer contentIndexingNumOfConsumers =
            Configurer.get("messaging.deer.content.indexer.consumers").toInt();
    private final Integer contentIndexingEquivalenceGraphChangesNumOfConsumers =
            Configurer.get("messaging.deer.content.indexer.graph.changes.consumers").toInt();

    private final String equivSystem = Configurer.get("equiv.update.producer.system").get();
    private final String equivTopic = Configurer.get("equiv.update.producer.topic").get();

    private final Boolean contentIndexerEnabled =
            Configurer.get("messaging.deer.content.indexer.enabled").toBoolean();
    private final Boolean equivalentContentStoreEnabled =
            Configurer.get("messaging.deer.equivalent.content.content.changes.enabled").toBoolean();
    private final Boolean equivalentContentGraphEnabled =
            Configurer.get("messaging.deer.equivalent.content.graph.changes.enabled").toBoolean();
    private final Boolean equivalentScheduleContentEnabled =
            Configurer.get("messaging.deer.equivalent.schedule.content.changes.enabled")
                    .toBoolean();
    private final Boolean equivalentScheduleScheduleEnabled =
            Configurer.get("messaging.deer.equivalent.schedule.schedule.changes.enabled")
                    .toBoolean();
    private final Boolean equivalentScheduleGraphEnabled =
            Configurer.get("messaging.deer.equivalent.schedule.graph.changes.enabled").toBoolean();
    private final Boolean topicIndexerEnabled =
            Configurer.get("messaging.deer.topic.indexer.enabled").toBoolean();
    private final Boolean equivalenceGraphEnabled =
            Configurer.get("messaging.deer.equivalence.content.graph.changes.enabled").toBoolean();

    @Autowired
    private KafkaMessagingModule messaging;
    @Autowired
    private AtlasPersistenceModule persistence;
    @Autowired
    private ProcessingMetricsModule metricsModule;

    private ServiceManager consumerManager;

    @Bean
    @Lazy
    public Worker<ResourceUpdatedMessage> topicIndexingWorker() {
        return new TopicIndexingWorker(
                persistence.topicStore(),
                persistence.topicIndex(),
                metricsModule.metrics()
        );
    }

    @Bean
    @Lazy
    public KafkaConsumer topicIndexerMessageListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(
                        topicIndexingWorker(),
                        serializer(ResourceUpdatedMessage.class),
                        topicChanges,
                        "TopicIndexer"
                )
                .withDefaultConsumers(topicIndexingNumOfConsumers)
                .withMaxConsumers(topicIndexingNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy
    public Worker<EquivalenceGraphUpdateMessage> equivalentContentStoreGraphUpdateWorker() {
        return new EquivalentContentStoreGraphUpdateWorker(
                persistence.getEquivalentContentStore(),
                metricsModule.metrics()
        );
    }

    @Bean
    @Lazy
    public KafkaConsumer equivalentContentStoreGraphUpdateListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(
                        equivalentContentStoreGraphUpdateWorker(),
                        serializer(EquivalenceGraphUpdateMessage.class),
                        contentEquivalenceGraphChanges,
                        "EquivalentContentStoreGraphs"
                )
                .withDefaultConsumers(equivContentGraphChangesNumOfConsumers)
                .withMaxConsumers(equivContentGraphChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy
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
    @Lazy
    public KafkaConsumer equivalentContentStoreContentUpdateListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(
                        equivalentContentStoreContentUpdateWorker(),
                        serializer(ResourceUpdatedMessage.class),
                        contentChanges,
                        "EquivalentContentStoreContent"
                )
                .withDefaultConsumers(equivContentContentChangesNumOfConsumers)
                .withMaxConsumers(equivContentContentChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy
    public Worker<EquivalenceGraphUpdateMessage> equivalentScheduleStoreGraphUpdateWorker() {
        return EquivalentScheduleStoreGraphUpdateWorker.create(
                persistence.getEquivalentScheduleStore(),
                metricsModule.metrics(),
                EquivalenceGraphUpdateResolver.create(persistence.getContentEquivalenceGraphStore())
        );
    }

    @Bean
    @Lazy
    public Worker<EquivalentContentUpdatedMessage> equivalentScheduleStoreContentUpdateWorker() {
        return new EquivalentScheduleStoreContentUpdateWorker(
                persistence.getEquivalentContentStore(),
                persistence.getEquivalentScheduleStore(),
                metricsModule.metrics()
        );
    }

    @Bean
    @Lazy
    public KafkaConsumer equivalentScheduleStoreGraphUpdateListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(
                        equivalentScheduleStoreGraphUpdateWorker(),
                        serializer(EquivalenceGraphUpdateMessage.class),
                        contentEquivalenceGraphChanges,
                        "EquivalentScheduleStoreGraphs"
                )
                .withDefaultConsumers(equivScheduleGraphChangesNumOfConsumers)
                .withMaxConsumers(equivScheduleGraphChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy
    public KafkaConsumer equivalentScheduleStoreContentListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(
                        equivalentScheduleStoreContentUpdateWorker(),
                        serializer(EquivalentContentUpdatedMessage.class),
                        equivalentContentChanges,
                        "EquivalentScheduleStoreContent"
                )
                .withDefaultConsumers(equivScheduleContentChangesNumOfConsumers)
                .withMaxConsumers(equivScheduleContentChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy
    public Worker<ScheduleUpdateMessage> equivalentScheduleStoreScheduleUpdateWorker() {
        return new EquivalentScheduleStoreScheduleUpdateWorker(
                persistence.getEquivalentScheduleStore(),
                metricsModule.metrics()
        );
    }

    @Bean
    @Lazy
    public KafkaConsumer equivalentScheduleStoreScheduleUpdateListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(
                        equivalentScheduleStoreScheduleUpdateWorker(),
                        serializer(ScheduleUpdateMessage.class),
                        scheduleChanges,
                        "EquivalentScheduleStoreSchedule"
                )
                .withDefaultConsumers(equivScheduleScheduleChangesNumOfConsumers)
                .withMaxConsumers(equivScheduleScheduleChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy
    public Worker<EquivalenceAssertionMessage> contentEquivalenceUpdater() {
        return new ContentEquivalenceUpdatingWorker(
                persistence.getContentEquivalenceGraphStore(),
                metricsModule.metrics(),
                explicitEquivalenceMigrator()
        );
    }

    @Bean
    @Lazy
    public KafkaConsumer equivUpdateListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(
                        contentEquivalenceUpdater(),
                        new ContentEquivalenceAssertionLegacyMessageSerializer(),
                        equivTopic,
                        "EquivGraphUpdate"
                )
                .withProducerSystem(equivSystem)
                .withDefaultConsumers(contentEquivalenceGraphChangesNumOfConsumers)
                .withMaxConsumers(contentEquivalenceGraphChangesNumOfConsumers)
                .withFailedMessagePersistence(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy
    public EquivalentContentIndexingContentWorker equivalentContentIndexingWorker() {
        return new EquivalentContentIndexingContentWorker(
                persistence.contentStore(), persistence.contentIndex(), metricsModule.metrics()
        );
    }

    @Bean
    @Lazy
    public KafkaConsumer equivalentContentIndexingMessageListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(
                        equivalentContentIndexingWorker(),
                        serializer(EquivalentContentUpdatedMessage.class),
                        equivalentContentChanges,
                        "EquivalentContentIndexer"
                )
                .withMaxConsumers(contentIndexingNumOfConsumers)
                .withDefaultConsumers(contentIndexingNumOfConsumers)
                .withConsumerSystem(consumerSystem)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @Bean
    @Lazy()
    public EquivalentContentIndexingGraphWorker equivalentContentIndexingGraphWorker() {
        return EquivalentContentIndexingGraphWorker.create(
                persistence.contentIndex(),
                metricsModule.metrics(),
                EquivalenceGraphUpdateResolver.create(persistence.getContentEquivalenceGraphStore())
        );
    }

    @Bean
    @Lazy()
    public KafkaConsumer equivalentContentIndexingGraphMessageListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(
                        equivalentContentIndexingGraphWorker(),
                        serializer(EquivalenceGraphUpdateMessage.class),
                        contentEquivalenceGraphChanges,
                        "EquivalentContentIndexer"
                )
                .withMaxConsumers(contentIndexingEquivalenceGraphChangesNumOfConsumers)
                .withDefaultConsumers(contentIndexingEquivalenceGraphChangesNumOfConsumers)
                .withConsumerSystem(consumerSystem)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .build();
    }

    @PostConstruct
    public void start() throws TimeoutException {
        ImmutableList.Builder<Service> services = ImmutableList.builder();

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
        if (equivalentScheduleContentEnabled) {
            services.add(equivalentScheduleStoreContentListener());
        }
        if (equivalentScheduleScheduleEnabled) {
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

    private <M extends Message> MessageSerializer<M> serializer(Class<M> cls) {
        return JacksonMessageSerializer.forType(cls);
    }

    private DirectAndExplicitEquivalenceMigrator explicitEquivalenceMigrator() {
        return new DirectAndExplicitEquivalenceMigrator(
                persistence.legacyContentResolver(),
                persistence.legacyEquivalenceStore(),
                persistence.getContentEquivalenceGraphStore()
        );
    }
}
