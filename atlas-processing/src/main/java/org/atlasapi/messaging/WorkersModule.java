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

import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageSerializer;
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

    private static final String WORKER_METRIC_PREFIX = "messaging.worker.";

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
    private final String equivalentContentGraphChanges = Configurer
            .get("messaging.destination.equivalent.content.graph.changes").get();

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
    private final Integer neo4jContentStoreContentUpdateNumOfConsumers =
            Configurer.get("messaging.deer.content.neo4j.content.changes.consumers").toInt();
    private final Integer neo4jContentStoreGraphUpdateNumOfConsumers =
            Configurer.get("messaging.deer.content.neo4j.graph.changes.consumers").toInt();

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
    private final Boolean neo4jContentStoreContentUpdateEnabled =
            Configurer.get("messaging.deer.content.neo4j.content.changes.enabled").toBoolean();
    private final Boolean neo4jContentStoreGraphUpdateEnabled =
            Configurer.get("messaging.deer.content.neo4j.graph.changes.enabled").toBoolean();

    @Autowired
    private KafkaMessagingModule messaging;
    @Autowired
    private AtlasPersistenceModule persistence;
    @Autowired
    private ProcessingMetricsModule metricsModule;

    private ServiceManager consumerManager;

    @Bean
    @Lazy
    public KafkaConsumer topicIndexerMessageListener() {
        String workerName = "TopicIndexer";

        return messaging.messageConsumerFactory()
                .createConsumer(
                        TopicIndexingWorker.create(
                                persistence.topicStore(),
                                persistence.topicIndex(),
                                WORKER_METRIC_PREFIX + workerName + ".",
                                metricsModule.metrics()
                        ),
                        serializer(ResourceUpdatedMessage.class),
                        topicChanges,
                        workerName
                )
                .withDefaultConsumers(topicIndexingNumOfConsumers)
                .withMaxConsumers(topicIndexingNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    public KafkaConsumer equivalentContentStoreGraphUpdateListener() {
        String workerName = "EquivalentContentStoreGraphs";

        return messaging.messageConsumerFactory()
                .createConsumer(
                        EquivalentContentStoreGraphUpdateWorker.create(
                                persistence.getEquivalentContentStore(),
                                WORKER_METRIC_PREFIX + workerName + ".",
                                metricsModule.metrics()
                        ),
                        serializer(EquivalenceGraphUpdateMessage.class),
                        contentEquivalenceGraphChanges,
                        workerName
                )
                .withDefaultConsumers(equivContentGraphChangesNumOfConsumers)
                .withMaxConsumers(equivContentGraphChangesNumOfConsumers)
                .withFailedMessagePersistence(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    public KafkaConsumer equivalentContentStoreContentUpdateListener() {
        String workerName = "EquivalentContentStoreContent";

        return messaging.messageConsumerFactory()
                .createConsumer(
                        EquivalentContentStoreContentUpdateWorker.create(
                                persistence.getEquivalentContentStore(),
                                WORKER_METRIC_PREFIX + workerName + ".",
                                metricsModule.metrics()
                        ),
                        serializer(ResourceUpdatedMessage.class),
                        contentChanges,
                        workerName
                )
                .withDefaultConsumers(equivContentContentChangesNumOfConsumers)
                .withMaxConsumers(equivContentContentChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    public KafkaConsumer equivalentScheduleStoreGraphUpdateListener() {
        String workerName = "EquivalentScheduleStoreGraphs";

        return messaging.messageConsumerFactory()
                .createConsumer(
                        EquivalentScheduleStoreGraphUpdateWorker.create(
                                persistence.getEquivalentScheduleStore(),
                                EquivalenceGraphUpdateResolver.create(
                                        persistence.getContentEquivalenceGraphStore()
                                ),
                                WORKER_METRIC_PREFIX + workerName + ".",
                                metricsModule.metrics()
                        ),
                        serializer(EquivalenceGraphUpdateMessage.class),
                        contentEquivalenceGraphChanges,
                        workerName
                )
                .withDefaultConsumers(equivScheduleGraphChangesNumOfConsumers)
                .withMaxConsumers(equivScheduleGraphChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    public KafkaConsumer equivalentScheduleStoreContentListener() {
        String workerName = "EquivalentScheduleStoreContent";

        return messaging.messageConsumerFactory()
                .createConsumer(
                        EquivalentScheduleStoreContentUpdateWorker.create(
                                persistence.getEquivalentContentStore(),
                                persistence.getEquivalentScheduleStore(),
                                WORKER_METRIC_PREFIX + workerName + ".",
                                metricsModule.metrics()
                        ),
                        serializer(EquivalentContentUpdatedMessage.class),
                        equivalentContentChanges,
                        workerName
                )
                .withDefaultConsumers(equivScheduleContentChangesNumOfConsumers)
                .withMaxConsumers(equivScheduleContentChangesNumOfConsumers)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    public KafkaConsumer equivalentScheduleStoreScheduleUpdateListener() {
        String workerName = "EquivalentScheduleStoreSchedule";

        return messaging.messageConsumerFactory()
                .createConsumer(
                        EquivalentScheduleStoreScheduleUpdateWorker.create(
                                persistence.getEquivalentScheduleStore(),
                                WORKER_METRIC_PREFIX + workerName + ".",
                                metricsModule.metrics()
                        ),
                        serializer(ScheduleUpdateMessage.class),
                        scheduleChanges,
                        workerName
                )
                .withDefaultConsumers(equivScheduleScheduleChangesNumOfConsumers)
                .withMaxConsumers(equivScheduleScheduleChangesNumOfConsumers)
                .withFailedMessagePersistence(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    public KafkaConsumer equivUpdateListener() {
        String workerName = "EquivGraphUpdate";

        return messaging.messageConsumerFactory()
                .createConsumer(
                        ContentEquivalenceUpdatingWorker.create(
                                persistence.getContentEquivalenceGraphStore(),
                                explicitEquivalenceMigrator(),
                                WORKER_METRIC_PREFIX + workerName + ".",
                                metricsModule.metrics()
                        ),
                        new ContentEquivalenceAssertionLegacyMessageSerializer(),
                        equivTopic,
                        workerName
                )
                .withProducerSystem(equivSystem)
                .withDefaultConsumers(contentEquivalenceGraphChangesNumOfConsumers)
                .withMaxConsumers(contentEquivalenceGraphChangesNumOfConsumers)
                .withFailedMessagePersistence(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    public KafkaConsumer equivalentContentIndexingMessageListener() {
        String workerName = "EquivalentContentIndexer";

        return messaging.messageConsumerFactory()
                .createConsumer(
                        EquivalentContentIndexingContentWorker.create(
                                persistence.contentStore(),
                                persistence.contentIndex(),
                                WORKER_METRIC_PREFIX + workerName + ".",
                                metricsModule.metrics()
                        ),
                        serializer(EquivalentContentUpdatedMessage.class),
                        equivalentContentChanges,
                        workerName
                )
                .withMaxConsumers(contentIndexingNumOfConsumers)
                .withDefaultConsumers(contentIndexingNumOfConsumers)
                .withConsumerSystem(consumerSystem)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy()
    public KafkaConsumer equivalentContentIndexingGraphMessageListener() {
        String workerName = "EquivalentContentIndexer";

        return messaging.messageConsumerFactory()
                .createConsumer(
                        EquivalentContentIndexingGraphWorker.create(
                                persistence.contentIndex(),
                                EquivalenceGraphUpdateResolver.create(
                                        persistence.getContentEquivalenceGraphStore()
                                ),
                                WORKER_METRIC_PREFIX + workerName + ".",
                                metricsModule.metrics()
                        ),
                        serializer(EquivalenceGraphUpdateMessage.class),
                        contentEquivalenceGraphChanges,
                        workerName
                )
                .withMaxConsumers(contentIndexingEquivalenceGraphChangesNumOfConsumers)
                .withDefaultConsumers(contentIndexingEquivalenceGraphChangesNumOfConsumers)
                .withConsumerSystem(consumerSystem)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    public KafkaConsumer neo4jContentStoreContentUpdateMessageListener() {
        String workerName = "Neo4jContentStoreContentUpdateWorker";

        Neo4jContentStoreContentUpdateWorker worker = Neo4jContentStoreContentUpdateWorker
                .create(
                        persistence.contentStore(),
                        persistence.neo4jContentStore(),
                        WORKER_METRIC_PREFIX + workerName + ".",
                        metricsModule.metrics()
                );

        return messaging.messageConsumerFactory()
                .createConsumer(
                        worker,
                        serializer(EquivalentContentUpdatedMessage.class),
                        equivalentContentChanges,
                        workerName
                )
                .withMaxConsumers(neo4jContentStoreContentUpdateNumOfConsumers)
                .withDefaultConsumers(neo4jContentStoreContentUpdateNumOfConsumers)
                .withConsumerSystem(consumerSystem)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
                .build();
    }

    @Bean
    @Lazy
    public KafkaConsumer neo4jContentStoreGraphUpdateMessageListener() {
        String workerName = "Neo4jContentStoreGraphUpdateWorker";

        Neo4jContentStoreGraphUpdateWorker worker = Neo4jContentStoreGraphUpdateWorker
                .create(
                        persistence.legacyContentResolver(),
                        persistence.legacyEquivalenceStore(),
                        persistence.neo4jContentStore(),
                        WORKER_METRIC_PREFIX + workerName + ".",
                        metricsModule.metrics()
                );

        return messaging.messageConsumerFactory()
                .createConsumer(
                        worker,
                        serializer(EquivalenceGraphUpdateMessage.class),
                        equivalentContentGraphChanges,
                        workerName
                )
                .withMaxConsumers(neo4jContentStoreGraphUpdateNumOfConsumers)
                .withDefaultConsumers(neo4jContentStoreGraphUpdateNumOfConsumers)
                .withConsumerSystem(consumerSystem)
                .withPersistentRetryPolicy(persistence.databasedWriteMongo())
                .withMetricRegistry(metricsModule.metrics())
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
        if (neo4jContentStoreContentUpdateEnabled) {
            services.add(neo4jContentStoreContentUpdateMessageListener());
        }
        if (neo4jContentStoreGraphUpdateEnabled) {
            services.add(neo4jContentStoreGraphUpdateMessageListener());
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
        return DirectAndExplicitEquivalenceMigrator.create(
                persistence.legacyContentResolver(),
                persistence.legacyEquivalenceStore(),
                persistence.getContentEquivalenceGraphStore()
        );
    }
}
