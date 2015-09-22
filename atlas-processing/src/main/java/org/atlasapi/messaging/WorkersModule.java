package org.atlasapi.messaging;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ServiceManager;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageConsumerBuilder;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;
import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Configuration
@Import({AtlasPersistenceModule.class, KafkaMessagingModule.class, ProcessingHealthModule.class})
public class WorkersModule {

    private String consumerSystem = Configurer.get("messaging.system").get();

    private String contentChanges = Configurer.get("messaging.destination.content.changes").get();
    private String topicChanges = Configurer.get("messaging.destination.topics.changes").get();
    private String scheduleChanges = Configurer.get("messaging.destination.schedule.changes").get();
    private String contentEquivalenceGraphChanges = Configurer.get("messaging.destination.equivalence.content.graph.changes").get();
    private String equivalentContentChanges = Configurer.get("messaging.destination.equivalent.content.changes").get();

    private Integer defaultTopicIndexers = Configurer.get("messaging.topic.indexing.consumers.default").toInt();
    private Integer maxTopicIndexers = Configurer.get("messaging.topic.indexing.consumers.max").toInt();

    private Integer defaultContentIndexers = Configurer.get("messaging.content.indexing.consumers.default").toInt();
    private Integer maxContentIndexers = Configurer.get("messaging.content.indexing.consumers.max").toInt();

    private Integer equivDefaultConsumers = Configurer.get("equiv.update.consumers.default").toInt();
    private Integer equivMaxConsumers = Configurer.get("equiv.update.consumers.max").toInt();

    private String equivSystem = Configurer.get("equiv.update.producer.system").get();
    private String equivTopic = Configurer.get("equiv.update.producer.topic").get();

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
    public Worker<ResourceUpdatedMessage> topicIndexingWorker() {
        return new TopicIndexingWorker(persistence.topicStore(), persistence.topicIndex(), health.metrics());
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer topicIndexerMessageListener() {
        return messaging.messageConsumerFactory().createConsumer(topicIndexingWorker(),
                serializer(ResourceUpdatedMessage.class), topicChanges, "TopicIndexer")
                .withDefaultConsumers(defaultTopicIndexers)
                .withMaxConsumers(maxTopicIndexers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalenceGraphUpdateMessage> equivalentContentStoreGraphUpdateWorker() {
        return new EquivalentContentStoreGraphUpdateWorker(persistence.getEquivalentContentStore(), health.metrics());
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentContentStoreGraphUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(equivalentContentStoreGraphUpdateWorker(),
                serializer(EquivalenceGraphUpdateMessage.class),
                contentEquivalenceGraphChanges, "EquivalentContentStoreGraphs")
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
                health.metrics()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentContentStoreContentUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(equivalentContentStoreContentUpdateWorker(),
                serializer(ResourceUpdatedMessage.class),
                contentChanges, "EquivalentContentStoreContent")
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalenceGraphUpdateMessage> equivalentScheduletStoreGraphUpdateWorker() {
        return new EquivalentScheduleStoreGraphUpdateWorker(persistence.getEquivalentScheduleStore(), health.metrics());
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalentContentUpdatedMessage> equivalentScheduletStoreContentUpdateWorker() {
        return new EquivalentScheduleStoreContentUpdateWorker(
                persistence.getEquivalentContentStore(),
                persistence.getEquivalentScheduleStore(),
                health.metrics()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentScheduleStoreGraphUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(equivalentScheduletStoreGraphUpdateWorker(),
                serializer(EquivalenceGraphUpdateMessage.class),
                contentEquivalenceGraphChanges, "EquivalentScheduleStoreGraphs")
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentScheduleStoreContentListener() {
        return messaging.messageConsumerFactory().createConsumer(equivalentScheduletStoreContentUpdateWorker(),
                serializer(EquivalentContentUpdatedMessage.class),
                equivalentContentChanges, "EquivalentScheduleStoreContent")
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<ScheduleUpdateMessage> equivalentScheduleStoreScheduleUpdateWorker() {
        return new EquivalentScheduleStoreScheduleUpdateWorker(persistence.getEquivalentScheduleStore(), health.metrics());
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentScheduleStoreScheduleUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(equivalentScheduleStoreScheduleUpdateWorker(),
                serializer(ScheduleUpdateMessage.class), scheduleChanges, "EquivalentScheduleStoreSchedule")
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalenceAssertionMessage> contentEquivalenceUpdater() {
        return new ContentEquivalenceUpdatingWorker(
                persistence.getContentEquivalenceGraphStore(),
                health.metrics(),
                explicitEquivalenceMigrator()
        );
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer equivUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(contentEquivalenceUpdater(),
                new ContentEquivalenceAssertionLegacyMessageSerializer(), equivTopic, "EquivGraphUpdate")
                .withProducerSystem(equivSystem)
                .withDefaultConsumers(equivDefaultConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public ContentIndexingWorker contentIndexingWorker() {
        return new ContentIndexingWorker(persistence.contentStore(), persistence.contentIndex(), health.metrics());
    }


    @Bean
    @Lazy(true)
    public KafkaConsumer contentIndexingMessageListener() {
        MessageConsumerBuilder<KafkaConsumer, ResourceUpdatedMessage> consumer =
                messaging.messageConsumerFactory().createConsumer(
                        contentIndexingWorker(),
                        serializer(ResourceUpdatedMessage.class),
                        contentChanges,
                        "ContentIndexer"
                );
        return consumer.withMaxConsumers(maxContentIndexers)
                .withDefaultConsumers(defaultContentIndexers)
                .withConsumerSystem(consumerSystem)
                .build();
    }

    @PostConstruct
    public void start() throws TimeoutException {
        consumerManager = new ServiceManager(ImmutableList.of(
                equivUpdateListener(),
                equivalentScheduleStoreScheduleUpdateListener(),
                equivalentScheduleStoreGraphUpdateListener(),
                equivalentContentStoreGraphUpdateListener(),
                equivalentContentStoreContentUpdateListener(),
//                equivalentScheduleStoreContentListener(),
                topicIndexerMessageListener(),
                contentIndexingMessageListener()
        ));
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