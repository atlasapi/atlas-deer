package org.atlasapi.system.bootstrap.workers;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.messaging.v3.JacksonMessageSerializer;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.bootstrap.ChannelIntervalScheduleBootstrapTaskFactory;
import org.atlasapi.system.legacy.LegacyPersistenceModule;
import org.atlasapi.topic.TopicResolver;
import org.atlasapi.topic.TopicStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;
import com.metabroadcast.common.queue.kafka.KafkaMessageConsumerFactory;

@Configuration
@Import({AtlasPersistenceModule.class, KafkaMessagingModule.class, LegacyPersistenceModule.class,
        ProcessingHealthModule.class})
public class BootstrapWorkersModule {

    private String consumerSystem = Configurer.get("messaging.system").get();
    private String zookeeper = Configurer.get("messaging.zookeeper").get();
    private String originSystem = Configurer.get("messaging.bootstrap.system").get();
    private Integer consumers = Configurer.get("messaging.bootstrap.consumers.default").toInt();
    private Integer maxConsumers = Configurer.get("messaging.bootstrap.consumers.max").toInt();
    private String contentChanges = Configurer.get("messaging.destination.content.changes").get();
    private String topicChanges = Configurer.get("messaging.destination.topics.changes").get();
    private Long backOffIntervalMillis = Configurer.get("messaging.maxBackOffMillis").toLong();
    private Long maxBackOffMillis = Configurer.get("messaging.maxBackOffMillis").toLong();

    private String scheduleChanges = Configurer.get("messaging.destination.schedule.changes").get();
    private Set<Publisher> ignoredScheduleSources
            = Sets.difference(Publisher.all(), ImmutableSet.of(Publisher.PA));
    @Autowired
    private AtlasPersistenceModule persistence;
    @Autowired
    private LegacyPersistenceModule legacy;
    @Autowired
    private KafkaMessagingModule messaging;
    @Autowired
    private ProcessingHealthModule health;

    @Bean
    @Qualifier("bootstrap")
    KafkaMessageConsumerFactory bootstrapQueueFactory() {
        return new KafkaMessageConsumerFactory(zookeeper, originSystem, backOffIntervalMillis, maxBackOffMillis);
    }

    @Bean
    @Lazy(true)
    KafkaConsumer contentReadWriter() {
        ContentResolver legacyResolver = legacy.legacyContentResolver();
        ContentReadWriteWorker worker = new ContentReadWriteWorker(
                legacyResolver,
                persistence.contentStore(),
                explicitEquivalenceMigrator(),
                health.metrics()
        );
        MessageSerializer<ResourceUpdatedMessage> serializer =
                new EntityUpdatedLegacyMessageSerializer();
        return bootstrapQueueFactory().createConsumer(worker, serializer, contentChanges, "ContentBootstrap")
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    KafkaConsumer scheduleReadWriter() {
        ScheduleReadWriteWorker worker = new ScheduleReadWriteWorker(scheduleBootstrapTaskFactory(),
                persistence.channelStore(), ignoredScheduleSources);
        MessageSerializer<ScheduleUpdateMessage> serializer
                = JacksonMessageSerializer.forType(ScheduleUpdateMessage.class);
        return bootstrapQueueFactory().createConsumer(worker, serializer, scheduleChanges, "ScheduleBootstrap")
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
        TopicReadWriteWorker worker = new TopicReadWriteWorker(legacyResolver, writer);
        MessageSerializer<ResourceUpdatedMessage> serializer =
                new EntityUpdatedLegacyMessageSerializer();
        return bootstrapQueueFactory().createConsumer(worker, serializer, topicChanges, "TopicBootstrap")
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
                .build();
    }

    @PostConstruct
    public void start() throws TimeoutException {
        contentReadWriter().startAsync().awaitRunning(1, TimeUnit.MINUTES);
        scheduleReadWriter().startAsync().awaitRunning(1, TimeUnit.MINUTES);
        topicReadWriter().startAsync().awaitRunning(1, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void stop() throws TimeoutException {
        contentReadWriter().stopAsync().awaitTerminated(1, TimeUnit.MINUTES);
        scheduleReadWriter().stopAsync().awaitTerminated(1, TimeUnit.MINUTES);
        topicReadWriter().stopAsync().awaitTerminated(1, TimeUnit.MINUTES);
    }

    @Bean
    public ChannelIntervalScheduleBootstrapTaskFactory scheduleBootstrapTaskFactory() {
        return new ChannelIntervalScheduleBootstrapTaskFactory(legacy.legacyScheduleStore(), persistence.scheduleStore(),
                new DelegatingContentStore(legacy.legacyContentResolver(), persistence.contentStore()));
    }


     public ExplicitEquivalenceMigrator explicitEquivalenceMigrator() {
        return new ExplicitEquivalenceMigrator(
                legacy.legacyContentResolver(),
                legacy.legacyEquivalenceStore(),
                persistence.getContentEquivalenceGraphStore()
        );
    }

}
