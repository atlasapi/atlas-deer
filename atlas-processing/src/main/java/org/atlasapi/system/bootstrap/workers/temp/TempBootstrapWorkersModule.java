package org.atlasapi.system.bootstrap.workers.temp;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.ElasticSearchContentIndexModule;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.messaging.v3.JacksonMessageSerializer;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.bootstrap.ChannelIntervalScheduleBootstrapTaskFactory;
import org.atlasapi.system.bootstrap.workers.ContentBootstrapWorker;
import org.atlasapi.system.bootstrap.workers.DelegatingContentStore;
import org.atlasapi.system.bootstrap.workers.EntityUpdatedLegacyMessageSerializer;
import org.atlasapi.system.bootstrap.workers.ScheduleReadWriteWorker;
import org.atlasapi.system.bootstrap.workers.TopicReadWriteWorker;
import org.atlasapi.system.legacy.LegacyPersistenceModule;
import org.atlasapi.topic.TopicResolver;
import org.atlasapi.topic.TopicStore;
import org.joda.time.Duration;
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
public class TempBootstrapWorkersModule {

    private String consumerSystem = Configurer.get("messaging.system").get();
    private String zookeeper = Configurer.get("messaging.zookeeper").get();
    private String originSystem = Configurer.get("messaging.bootstrap.system").get();

    private Integer consumers = 25;
    private Integer maxConsumers = 25;

    private String contentChanges = "AtlasOwlProd" + Configurer.get("messaging.destination.content.changes").get();
    private String topicChanges = "AtlasOwlProd" + Configurer.get("messaging.destination.topics.changes").get();
    private String scheduleChanges = "AtlasOwlProd" + Configurer.get("messaging.destination.schedule.changes").get();

    private Duration backOffBase = Duration.millis(Configurer.get("messaging.maxBackOffMillis")
            .toLong());
    private Duration maxBackOff = Duration.millis(Configurer.get("messaging.maxBackOffMillis")
            .toLong());

    private Boolean v2ScheduleEnabled = Configurer.get("schedule.v2.enabled").toBoolean();

    private Boolean contentBootstrapEnabled = Configurer.get("messaging.enabled.content.bootstrap").toBoolean();
    private Boolean scheduleBootstrapEnabled = Configurer.get("messaging.enabled.schedule.bootstrap").toBoolean();
    private Boolean topicBootstrapEnabled = Configurer.get("messaging.enabled.topic.bootstrap").toBoolean();

    private Set<Publisher> ignoredScheduleSources
            = Sets.difference(Publisher.all(), ImmutableSet.of(Publisher.PA, Publisher.BBC_NITRO, Publisher.BT_BLACKOUT));

    @Autowired
    private AtlasPersistenceModule persistence;
    @Autowired
    private LegacyPersistenceModule legacy;
    @Autowired
    private KafkaMessagingModule messaging;
    @Autowired
    private ProcessingHealthModule health;
    @Autowired
    private ElasticSearchContentIndexModule search;

    @Bean
    @Qualifier("bootstrap")
    KafkaMessageConsumerFactory tempBootstrapQueueFactory() {
        return new KafkaMessageConsumerFactory(zookeeper, originSystem, backOffBase, maxBackOff);
    }

    @Bean
    @Lazy(true)
    KafkaConsumer tempContentBootstrapWorker() {
        ContentResolver legacyResolver = legacy.legacyContentResolver();
        ContentBootstrapWorker worker = new ContentBootstrapWorker(
                legacyResolver,
                persistence.contentStore(),
                health.metrics()
        );
        MessageSerializer<ResourceUpdatedMessage> serializer =
                new EntityUpdatedLegacyMessageSerializer();
        return tempBootstrapQueueFactory().createConsumer(worker,
                serializer,
                contentChanges,
                "TempContentBootstrap")
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    KafkaConsumer tempScheduleReadWriter() {
        ScheduleReadWriteWorker worker = new ScheduleReadWriteWorker(
                tempScheduleBootstrapTaskFactory(),
                persistence.channelResolver(), ignoredScheduleSources);
        MessageSerializer<ScheduleUpdateMessage> serializer
                = JacksonMessageSerializer.forType(ScheduleUpdateMessage.class);
        return tempBootstrapQueueFactory().createConsumer(worker,
                serializer,
                scheduleChanges,
                "TempScheduleBootstrap")
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    KafkaConsumer tempScheduleV2ReadWriter() {
        ScheduleReadWriteWorker worker = new ScheduleReadWriteWorker(
                tempScheduleV2BootstrapTaskFactory(),
                persistence.channelResolver(), ignoredScheduleSources);
        MessageSerializer<ScheduleUpdateMessage> serializer
                = JacksonMessageSerializer.forType(ScheduleUpdateMessage.class);
        return tempBootstrapQueueFactory().createConsumer(worker,
                serializer,
                scheduleChanges,
                "TempScheduleBootstrapV2")
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
                .build();
    }

    @Bean
    @Lazy(true)
    KafkaConsumer tempTopicReadWriter() {
        TopicResolver legacyResolver = legacy.legacyTopicResolver();
        TopicStore writer = persistence.topicStore();
        TopicReadWriteWorker worker = new TopicReadWriteWorker(legacyResolver, writer);
        MessageSerializer<ResourceUpdatedMessage> serializer =
                new EntityUpdatedLegacyMessageSerializer();
        return tempBootstrapQueueFactory().createConsumer(worker,
                serializer,
                topicChanges,
                "TempTopicBootstrap")
                .withConsumerSystem(consumerSystem)
                .withDefaultConsumers(consumers)
                .withMaxConsumers(maxConsumers)
                .build();
    }

    @PostConstruct
    public void start() throws TimeoutException {
        if(contentBootstrapEnabled) {
            tempContentBootstrapWorker().startAsync().awaitRunning(1, TimeUnit.MINUTES);
        }
        if(scheduleBootstrapEnabled) {
            tempScheduleReadWriter().startAsync().awaitRunning(1, TimeUnit.MINUTES);
        }
        if(v2ScheduleEnabled) {
            tempScheduleV2ReadWriter().startAsync().awaitRunning(1, TimeUnit.MINUTES);
        }
        if(topicBootstrapEnabled) {
            tempTopicReadWriter().startAsync().awaitRunning(1, TimeUnit.MINUTES);
        }
    }

    @PreDestroy
    public void stop() throws TimeoutException {
        if(contentBootstrapEnabled) {
            tempContentBootstrapWorker().stopAsync().awaitTerminated(1, TimeUnit.MINUTES);
        }
        if(scheduleBootstrapEnabled) {
            tempScheduleReadWriter().stopAsync().awaitTerminated(1, TimeUnit.MINUTES);
        }
        if(v2ScheduleEnabled) {
            tempScheduleV2ReadWriter().stopAsync().awaitRunning(1, TimeUnit.MINUTES);
        }
        if(topicBootstrapEnabled) {
            tempTopicReadWriter().stopAsync().awaitTerminated(1, TimeUnit.MINUTES);
        }
    }

    @Bean
    public ChannelIntervalScheduleBootstrapTaskFactory tempScheduleBootstrapTaskFactory() {
        return new ChannelIntervalScheduleBootstrapTaskFactory(legacy.legacyScheduleStore(), persistence.scheduleStore(),
                new DelegatingContentStore(legacy.legacyContentResolver(), persistence.contentStore()));
    }

    @Bean
    public ChannelIntervalScheduleBootstrapTaskFactory tempScheduleV2BootstrapTaskFactory() {
        return new ChannelIntervalScheduleBootstrapTaskFactory(legacy.legacyScheduleStore(), persistence.v2ScheduleStore(),
                new DelegatingContentStore(legacy.legacyContentResolver(), persistence.contentStore()));
    }
}
