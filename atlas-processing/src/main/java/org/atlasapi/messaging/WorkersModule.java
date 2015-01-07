package org.atlasapi.messaging;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.schedule.ScheduleUpdateMessage;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.bootstrap.workers.ContentEquivalenceAssertionLegacyMessageSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ServiceManager;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;

@Configuration
@Import({AtlasPersistenceModule.class, KafkaMessagingModule.class, ProcessingHealthModule.class})
public class WorkersModule {
    
    private String contentChanges = Configurer.get("messaging.destination.content.changes").get();
    private String topicChanges = Configurer.get("messaging.destination.topics.changes").get();
    private String scheduleChanges = Configurer.get("messaging.destination.schedule.changes").get();
    private String contentEquivalenceGraphChanges = Configurer.get("messaging.destination.equivalence.content.graph.changes").get();
    
    private Integer defaultIndexingConsumers = Configurer.get("messaging.indexing.consumers.default").toInt();
    private Integer maxIndexingConsumers = Configurer.get("messaging.indexing.consumers.max").toInt();
    
    private String equivSystem = Configurer.get("equiv.update.producer.system").get();
    private String equivTopic = Configurer.get("equiv.update.producer.topic").get();
    private Integer equivDefltConsumers = Configurer.get("equiv.update.consumers.default").toInt();
    private Integer equivMaxConsumers = Configurer.get("equiv.update.consumers.max").toInt();
    
//    private String loggerDestination = Configurer.get("messaging.destination.logger").get();
//    private int loggerConsumers = Integer.parseInt(Configurer.get("messaging.consumers.logger").get());
//    private long replayInterruptThreshold = Long.parseLong(Configurer.get("messaging.replay.interrupt.threshold").get());

    @Autowired private KafkaMessagingModule messaging;
    @Autowired private AtlasPersistenceModule persistence;
    @Autowired private ProcessingHealthModule health;
    private ServiceManager consumerManager;

    @Bean
    @Lazy(true)
    public Worker<ResourceUpdatedMessage> contentIndexingWorker() {
        return new ContentIndexingWorker(persistence.contentStore(), persistence.contentIndex());
    }

    @Bean
    @Lazy(true)
    public KafkaConsumer contentIndexerMessageListener() {
        return messaging.messageConsumerFactory().createConsumer(contentIndexingWorker(), 
                serializer(ResourceUpdatedMessage.class), contentChanges, "ContentIndexer")
                .withDefaultConsumers(defaultIndexingConsumers)
                .withMaxConsumers(maxIndexingConsumers)
                .build();
    }

    private <M extends Message> MessageSerializer<M> serializer(Class<M> cls) {
        return JacksonMessageSerializer.forType(cls);
    }

    @Bean
    @Lazy(true)
    public Worker<ResourceUpdatedMessage> topicIndexingWorker() {
        return new TopicIndexingWorker(persistence.topicStore(), persistence.topicIndex());
    }
    
    @Bean
    @Lazy(true)
    public KafkaConsumer topicIndexerMessageListener() {
        return messaging.messageConsumerFactory().createConsumer(topicIndexingWorker(), 
                serializer(ResourceUpdatedMessage.class), topicChanges, "TopicIndexer")
                .withDefaultConsumers(defaultIndexingConsumers)
                .withMaxConsumers(maxIndexingConsumers)
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
                .withDefaultConsumers(equivDefltConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<ResourceUpdatedMessage> equivalentContentStoreContentUpdateWorker() {
        return new EquivalentContentStoreContentUpdateWorker(persistence.getEquivalentContentStore(), health.metrics());
    }
    
    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentContentStoreContentUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(equivalentContentStoreContentUpdateWorker(),
                serializer(ResourceUpdatedMessage.class), 
                contentChanges, "EquivalentContentStoreContent")
                .withDefaultConsumers(equivDefltConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    public Worker<EquivalenceGraphUpdateMessage> equivalentScheduletStoreGraphUpdateWorker() {
        return new EquivalentScheduleStoreGraphUpdateWorker(persistence.getEquivalentScheduleStore());
    }
    
    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentScheduleStoreGraphUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(equivalentScheduletStoreGraphUpdateWorker(),
                serializer(EquivalenceGraphUpdateMessage.class), 
                contentEquivalenceGraphChanges, "EquivalentScheduleStoreGraphs")
                .withDefaultConsumers(equivDefltConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }
    
    @Bean
    @Lazy(true)
    public Worker<ScheduleUpdateMessage> equivalentScheduleStoreScheduleUpdateWorker() {
        return new EquivalentScheduleStoreScheduleUpdateWorker(persistence.getEquivalentScheduleStore());
    }
    
    @Bean
    @Lazy(true)
    public KafkaConsumer equivalentScheduleStoreScheduleUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(equivalentScheduleStoreScheduleUpdateWorker(), 
                serializer(ScheduleUpdateMessage.class), scheduleChanges, "EquivalentScheduleStoreSchedule")
                .withDefaultConsumers(equivDefltConsumers)
                .withMaxConsumers(equivMaxConsumers)
                .build();
    }


//    @Bean
//    @Lazy(true)
//    public Worker messageLogger() {
//        return new MessageLogger(messageStore);
//    }
//
//    @Bean
//    @Lazy(true)
//    public DefaultMessageListenerContainer messageLoggerMessageListener() {
//        return makeContainer(messageLogger(), loggerDestination, loggerConsumers, loggerConsumers);
//    }

    @Bean
    @Lazy(true)
    public Worker<EquivalenceAssertionMessage> contentEquivalenceUpdater() {
        return new ContentEquivalenceUpdatingWorker(persistence.getContentEquivalenceGraphStore(), health.metrics());
    }
    
    @Bean
    @Lazy(true)
    public KafkaConsumer equivUpdateListener() {
        return messaging.messageConsumerFactory().createConsumer(contentEquivalenceUpdater(), 
                new ContentEquivalenceAssertionLegacyMessageSerializer(), equivTopic, "EquivGraphUpdate")
                .withProducerSystem(equivSystem)
                .withDefaultConsumers(equivDefltConsumers)
                .withMaxConsumers(equivMaxConsumers)
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
            topicIndexerMessageListener(),
            contentIndexerMessageListener()
        ));
        consumerManager.startAsync().awaitHealthy(1, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void stop() throws TimeoutException {
       consumerManager.stopAsync().awaitStopped(1, TimeUnit.MINUTES);
    }

}
