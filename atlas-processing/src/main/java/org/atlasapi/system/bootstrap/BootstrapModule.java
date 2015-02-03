package org.atlasapi.system.bootstrap;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.SchedulerModule;
import org.atlasapi.content.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.system.bootstrap.workers.BootstrapWorkersModule;
import org.atlasapi.system.bootstrap.workers.DelegatingContentStore;
import org.atlasapi.system.legacy.LegacyPersistenceModule;
import org.atlasapi.topic.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DayRangeGenerator;
import com.metabroadcast.common.time.SystemClock;

@Configuration
@Import({AtlasPersistenceModule.class, BootstrapWorkersModule.class, LegacyPersistenceModule.class,
    SchedulerModule.class})
public class BootstrapModule {

    @Autowired private AtlasPersistenceModule persistence;
    @Autowired private LegacyPersistenceModule legacy;
    @Autowired private BootstrapWorkersModule workers;
    @Autowired private SchedulerModule scheduler;

    @Bean
    BootstrapController bootstrapController() {
        BootstrapController bootstrapController = new BootstrapController();
        
        bootstrapController.addBootstrapPair("legacy-content", new ResourceBootstrapper<Content>(legacy.legacyContentLister()), 
            new BootstrapListenerFactory<Content>() {
                @Override
                public BootstrapListener<Content> buildWithConcurrency(int concurrencyLevel) {
                    return new ContentWritingBootstrapListener(concurrencyLevel, persistence.contentStore());
                }
            }
        );
        bootstrapController.addBootstrapPair("legacy-topics", new ResourceBootstrapper<Topic>(legacy.legacyTopicLister()), 
            new BootstrapListenerFactory<Topic>() {
                @Override
                public BootstrapListener<Topic> buildWithConcurrency(int concurrencyLevel) {
                    return new TopicWritingBootstrapListener(concurrencyLevel, persistence.topicStore());
                }
            }
        );
        return bootstrapController;
    }

    @Bean
    IndividualContentBootstrapController contentBootstrapController() {
        return new IndividualContentBootstrapController(legacy.legacyContentResolver(), persistence.contentStore());
    }

    @Bean
    IndividualTopicBootstrapController topicBootstrapController() {
        return new IndividualTopicBootstrapController(legacy.legacyTopicResolver(), persistence.topicStore());
    }
    
    @Bean
    ExecutorServiceScheduledTask<UpdateProgress> scheduleBootstrapTask() {
        ChannelIntervalScheduleBootstrapTaskFactory taskFactory = workers.scheduleBootstrapTaskFactory();
        DayRangeGenerator dayRangeGenerator = new DayRangeGenerator().withLookAhead(7).withLookBack(7);
        Set<Publisher> sources = ImmutableSet.of(Publisher.PA);
        Supplier<Iterable<ChannelIntervalScheduleBootstrapTask>> supplier = 
            new SourceChannelIntervalTaskSupplier<ChannelIntervalScheduleBootstrapTask>(taskFactory, persistence.channelStore(), dayRangeGenerator, sources, new SystemClock());
        ExecutorService executor = Executors.newFixedThreadPool(10, 
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("schedule-bootstrap-%d").build());
        return new ExecutorServiceScheduledTask<UpdateProgress>(executor, supplier, 10, 1, TimeUnit.MINUTES);
    }
    
    @Bean
    ChannelIntervalScheduleBootstrapTaskFactory scheduleBootstrapTaskFactory() {
        return new ChannelIntervalScheduleBootstrapTaskFactory(legacy.legacyScheduleStore(), persistence.scheduleStore(), 
            new DelegatingContentStore(legacy.legacyContentResolver(), persistence.contentStore()));
    }
    
    @PostConstruct
    public void schedule() {
        scheduler.scheduler().schedule(scheduleBootstrapTask(), RepetitionRules.NEVER);
    }
    
    @Bean
    public ScheduleBootstrapController scheduleBootstrapController() {
        return new ScheduleBootstrapController(workers.scheduleBootstrapTaskFactory(), persistence.channelStore());
    }
}
