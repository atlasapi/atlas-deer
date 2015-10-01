package org.atlasapi.messaging.temp;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.messaging.ContentEquivalenceUpdatingWorker;
import org.atlasapi.messaging.ContentIndexingWorker;
import org.atlasapi.messaging.EquivalenceAssertionMessage;
import org.atlasapi.messaging.EquivalentContentIndexingWorker;
import org.atlasapi.messaging.EquivalentContentStoreContentUpdateWorker;
import org.atlasapi.messaging.EquivalentContentStoreGraphUpdateWorker;
import org.atlasapi.messaging.EquivalentContentUpdatedMessage;
import org.atlasapi.messaging.EquivalentScheduleStoreContentUpdateWorker;
import org.atlasapi.messaging.EquivalentScheduleStoreGraphUpdateWorker;
import org.atlasapi.messaging.EquivalentScheduleStoreScheduleUpdateWorker;
import org.atlasapi.messaging.JacksonMessageSerializer;
import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.messaging.TopicIndexingWorker;
import org.atlasapi.schedule.ScheduleUpdateMessage;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.bootstrap.workers.ContentEquivalenceAssertionLegacyMessageSerializer;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.bootstrap.workers.LegacyRetryingContentResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageConsumerBuilder;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;

@Configuration
@Import({AtlasPersistenceModule.class, KafkaMessagingModule.class, ProcessingHealthModule.class})
public class TempWorkersModule {

    private String consumerSystem = Configurer.get("messaging.system").get();

    private String contentChanges = "AtlasDeerProd" + Configurer.get("messaging.destination.content.changes").get();
    private String topicChanges = "AtlasDeerProd" + Configurer.get("messaging.destination.topics.changes").get();
    private String scheduleChanges = "AtlasDeerProd" + Configurer.get("messaging.destination.schedule.changes").get();
    private String contentEquivalenceGraphChanges = "AtlasDeerProd" + Configurer.get("messaging.destination.equivalence.content.graph.changes").get();
    private String equivalentContentChanges = "AtlasDeerProd" + Configurer.get("messaging.destination.equivalent.content.changes").get();

    private Integer defaultTopicIndexers = 25;
    private Integer maxTopicIndexers = 25;

    private Integer defaultContentIndexers = 25;
    private Integer maxContentIndexers = 25;

    private Integer equivDefaultConsumers = 25;
    private Integer equivMaxConsumers = 25;

    private String equivSystem = Configurer.get("equiv.update.producer.system").get();
    private String equivTopic = "AtlasOwlProd" + Configurer.get("equiv.update.producer.topic").get();

    private Boolean equivalentScheduleStoreContentUpdatesEnabled  = Configurer.get("equiv.schedule.content.updates.enabled").toBoolean();

    private Boolean contentIndexerEnabled = Configurer.get("messaging.enabled.content.indexer").toBoolean();
    private Boolean equivalentContentStoreEnabled = Configurer.get("messaging.enabled.content.equivalent.store").toBoolean();
    private Boolean equivalentContentGraphEnabled = Configurer.get("messaging.enabled.content.equivalent.graph").toBoolean();
    private Boolean equivalentScheduleStoreEnabled = Configurer.get("messaging.enabled.schedule.equivalent.store").toBoolean();
    private Boolean equivalentScheduleGraphEnabled = Configurer.get("messaging.enabled.schedule.equivalent.graph").toBoolean();
    private Boolean topicIndexerEnabled = Configurer.get("messaging.enabled.topic.indexer").toBoolean();
    private Boolean equivalenceGraphEnabled = Configurer.get("messaging.enabled.equivalence.graph").toBoolean();

    @Autowired
    private KafkaMessagingModule messaging;
    @Autowired
    private AtlasPersistenceModule persistence;
    @Autowired
    private ProcessingHealthModule health;
    private ServiceManager consumerManager;

    private <M extends Message> MessageSerializer<M> serializer(Class<M> cls) {
        return JacksonMessageSerializer.forType(cls);
    }

    @Bean
    @Lazy(true)
    public Worker<ResourceUpdatedMessage> tempTopicIndexingWorker() {
        return new TopicIndexingWorker(persistence.topicStore(), persistence.topicIndex(), health.metrics());
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer tempTopicIndexerMessageListener() {
        return messaging.messageConsumerFactory().createConsumer(tempTopicIndexingWorker(),
                serializer(ResourceUpdatedMessage.class), topicChanges, "TempTopicIndexer")
                .withDefaultConsumers(defaultTopicIndexers)
                .withMaxConsumers(maxTopicIndexers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalenceGraphUpdateMessage> tempEquivalentContentStoreGraphUpdateWorker() {
        return new EquivalentContentStoreGraphUpdateWorker(persistence.getEquivalentContentStore(), health.metrics());
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer tempEquivalentContentStoreGraphUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(
                tempEquivalentContentStoreGraphUpdateWorker(),
                serializer(EquivalenceGraphUpdateMessage.class),
                contentEquivalenceGraphChanges, "TempEquivalentContentStoreGraphs")
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<ResourceUpdatedMessage> tempEquivalentContentStoreContentUpdateWorker() {
        return new EquivalentContentStoreContentUpdateWorker(
                persistence.getEquivalentContentStore(),
                new LegacyRetryingContentResolver(
                        persistence.contentStore(),
                        persistence.legacyContentResolver(),
                        persistence.nullMessageSendingContentStore()
                ),
                health.metrics()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer tempEquivalentContentStoreContentUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(
                tempEquivalentContentStoreContentUpdateWorker(),
                serializer(ResourceUpdatedMessage.class),
                contentChanges, "TempEquivalentContentStoreContent")
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalenceGraphUpdateMessage> tempEquivalentScheduleStoreGraphUpdateWorker() {
        return new EquivalentScheduleStoreGraphUpdateWorker(persistence.getEquivalentScheduleStore(), health.metrics());
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalentContentUpdatedMessage> tempEquivalentScheduleStoreContentUpdateWorker() {
        return new EquivalentScheduleStoreContentUpdateWorker(
                persistence.getEquivalentContentStore(),
                persistence.getEquivalentScheduleStore(),
                health.metrics()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer tempEquivalentScheduleStoreGraphUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(
                tempEquivalentScheduleStoreGraphUpdateWorker(),
                serializer(EquivalenceGraphUpdateMessage.class),
                contentEquivalenceGraphChanges, "TempEquivalentScheduleStoreGraphs")
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer tempEquivalentScheduleStoreContentListener() {
        return messaging.messageConsumerFactory().createConsumer(
                tempEquivalentScheduleStoreContentUpdateWorker(),
                serializer(EquivalentContentUpdatedMessage.class),
                equivalentContentChanges, "TempEquivalentScheduleStoreContent")
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<ScheduleUpdateMessage> tempEquivalentScheduleStoreScheduleUpdateWorker() {
        return new EquivalentScheduleStoreScheduleUpdateWorker(persistence.getEquivalentScheduleStore(), health.metrics());
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer tempEquivalentScheduleStoreScheduleUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(
                tempEquivalentScheduleStoreScheduleUpdateWorker(),
                serializer(ScheduleUpdateMessage.class), scheduleChanges, "TempEquivalentScheduleStoreSchedule")
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalenceAssertionMessage> tempContentEquivalenceUpdater() {
        return new ContentEquivalenceUpdatingWorker(
                persistence.getContentEquivalenceGraphStore(),
                health.metrics(),
                explicitEquivalenceMigrator()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer tempEquivUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(tempContentEquivalenceUpdater(),
                new ContentEquivalenceAssertionLegacyMessageSerializer(),
                equivTopic,
                "TempEquivGraphUpdate")
                .withProducerSystem(equivSystem)
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public ContentIndexingWorker tempContentIndexingWorker() {
        return new ContentIndexingWorker(persistence.contentStore(), persistence.contentIndex(), health.metrics());
    }

    @Bean
    @Lazy(true)
    public EquivalentContentIndexingWorker tempEquivalentContentIndexingWorker() {
        return new EquivalentContentIndexingWorker(
                persistence.contentStore(), persistence.contentIndex(), health.metrics()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer tempContentIndexingMessageListener() {
        MessageConsumerBuilder<KafkaConsumer, ResourceUpdatedMessage> consumer =
                messaging.messageConsumerFactory().createConsumer(
                        tempContentIndexingWorker(),
                        serializer(ResourceUpdatedMessage.class),
                        contentChanges,
                        "TempContentIndexer"
                );
        return consumer.withMaxConsumers(maxContentIndexers)
                .withDefaultConsumers(defaultContentIndexers)
                .withConsumerSystem(consumerSystem)
                .build();
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer tempEquivalentContentIndexingMessageListener() {
        MessageConsumerBuilder<KafkaConsumer, EquivalentContentUpdatedMessage> consumer =
                messaging.messageConsumerFactory().createConsumer(
                        tempEquivalentContentIndexingWorker(),
                        serializer(EquivalentContentUpdatedMessage.class),
                        equivalentContentChanges,
                        "TempEquivalentContentIndexer"
                );
        return consumer.withMaxConsumers(maxContentIndexers)
                .withDefaultConsumers(defaultContentIndexers)
                .withConsumerSystem(consumerSystem)
                .build();
    }

    @PostConstruct
    public void start() throws TimeoutException {
        ImmutableList.Builder<Service> services = ImmutableList.builder();

        if(equivalentScheduleStoreContentUpdatesEnabled) {
            services.add(tempEquivalentScheduleStoreContentListener());
        }

        if(contentIndexerEnabled) {
            services.add(tempContentIndexingMessageListener());
            services.add(tempEquivalentContentIndexingMessageListener());
        }
        if(equivalentContentStoreEnabled) {
            services.add(tempEquivalentContentStoreContentUpdateListener());
        }
        if(equivalentContentGraphEnabled) {
            services.add(tempEquivalentContentStoreGraphUpdateListener());
        }
        if(equivalentScheduleStoreEnabled) {
            services.add(tempEquivalentScheduleStoreScheduleUpdateListener());
        }
        if(equivalentScheduleGraphEnabled) {
            services.add(tempEquivalentScheduleStoreGraphUpdateListener());
        }
        if(topicIndexerEnabled) {
            services.add(tempTopicIndexerMessageListener());
        }
        if(equivalenceGraphEnabled) {
            services.add(tempEquivUpdateListener());
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