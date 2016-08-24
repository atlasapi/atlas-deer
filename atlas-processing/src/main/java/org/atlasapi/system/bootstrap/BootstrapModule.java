package org.atlasapi.system.bootstrap;

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.ElasticSearchContentIndexModule;
import org.atlasapi.SchedulerModule;
import org.atlasapi.content.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.bootstrap.workers.BootstrapWorkersModule;
import org.atlasapi.system.bootstrap.workers.DelegatingContentStore;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.legacy.MongoProgressStore;
import org.atlasapi.system.legacy.ProgressStore;
import org.atlasapi.topic.Topic;

import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DayRangeGenerator;
import com.metabroadcast.common.time.SystemClock;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        AtlasPersistenceModule.class, BootstrapWorkersModule.class,
        SchedulerModule.class, ProcessingHealthModule.class })
public class BootstrapModule {

    //we only need 2 here, on to run the bootstrap and one to be able to return quickly when it's running
    private static final Integer NUMBER_OF_SCHECHULE_CONTROLLER_THREADS = 2;
    private static final Integer NUMBER_OF_SCHEDULE_BOOTSTRAP_THREADS = Configurer.get(
            "boootstrap.schedule.numThreads").toInt();
    private static final Integer NUMBER_OF_SOURCE_BOOTSTRAP_TRHEADS = Configurer.get(
            "boootstrap.source.numThreads").toInt();

    @Autowired private AtlasPersistenceModule persistence;
    @Autowired private BootstrapWorkersModule workers;
    @Autowired private SchedulerModule scheduler;
    @Autowired private ElasticSearchContentIndexModule search;
    @Autowired private MetricRegistry metrics;
    @Autowired private DirectAndExplicitEquivalenceMigrator explicitEquivalenceMigrator;

    @Bean
    BootstrapController bootstrapController() {
        BootstrapController bootstrapController = new BootstrapController();

        bootstrapController.addBootstrapPair("legacy-content",
                new ResourceBootstrapper<Content>(persistence.legacyContentLister()),
                new BootstrapListenerFactory<Content>() {

                    @Override
                    public BootstrapListener<Content> buildWithConcurrency(int concurrencyLevel) {
                        return new ContentWritingBootstrapListener(
                                concurrencyLevel,
                                persistence.contentStore()
                        );
                    }
                }
        );
        bootstrapController.addBootstrapPair("legacy-topics",
                new ResourceBootstrapper<Topic>(persistence.legacyTopicLister()),
                new BootstrapListenerFactory<Topic>() {

                    @Override
                    public BootstrapListener<Topic> buildWithConcurrency(int concurrencyLevel) {
                        return new TopicWritingBootstrapListener(
                                concurrencyLevel,
                                persistence.topicStore()
                        );
                    }
                }
        );
        return bootstrapController;
    }

    @Bean
    ContentBootstrapController contentBootstrapController() {
        return new ContentBootstrapController(
                persistence.legacyContentResolver(),
                persistence.legacyContentLister(),
                persistence.nullMessageSendingContentStore(),
                search.equivContentIndex(),
                explicitEquivalenceMigrator,
                NUMBER_OF_SOURCE_BOOTSTRAP_TRHEADS,
                progressStore(),
                metrics,
                persistence.nullMessageSendingEquivalentContentStore(),
                persistence.nullMessageSendingEquivalenceGraphStore(),
                persistence.contentStore(),
                persistence.neo4jContentStore()
        );
    }

    @Bean
    public ProgressStore progressStore() {
        return new MongoProgressStore(persistence.databasedWriteMongo());
    }

    @Bean
    IndividualTopicBootstrapController topicBootstrapController() {
        return new IndividualTopicBootstrapController(
                persistence.legacyTopicResolver(),
                persistence.topicStore()
        );
    }

    @Bean
    ExecutorServiceScheduledTask<UpdateProgress> scheduleBootstrapTask() {
        ChannelIntervalScheduleBootstrapTaskFactory taskFactory = workers.scheduleBootstrapTaskFactory();
        DayRangeGenerator dayRangeGenerator = new DayRangeGenerator().withLookAhead(7)
                .withLookBack(7);
        Set<Publisher> sources = ImmutableSet.of(Publisher.PA);
        Supplier<Iterable<ChannelIntervalScheduleBootstrapTask>> supplier =
                new SourceChannelIntervalTaskSupplier<ChannelIntervalScheduleBootstrapTask>(
                        taskFactory,
                        persistence.channelResolver(),
                        dayRangeGenerator,
                        sources,
                        new SystemClock()
                );
        ExecutorService executor = Executors.newFixedThreadPool(
                10,
                new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat("schedule-bootstrap-%d")
                        .build()
        );
        return new ExecutorServiceScheduledTask<UpdateProgress>(
                executor,
                supplier,
                10,
                1,
                TimeUnit.MINUTES
        );
    }

    @Bean
    ChannelIntervalScheduleBootstrapTaskFactory scheduleBootstrapTaskFactory() {
        return new ChannelIntervalScheduleBootstrapTaskFactory(persistence.legacyScheduleStore(),
                persistence.scheduleStore(),
                new DelegatingContentStore(
                        persistence.legacyContentResolver(),
                        persistence.contentStore()
                )
        );
    }

    @PostConstruct
    public void schedule() {
        scheduler.scheduler().schedule(scheduleBootstrapTask(), RepetitionRules.NEVER);
    }

    @Bean
    public ScheduleBootstrapController scheduleBootstrapController() {
        return new ScheduleBootstrapController(
                persistence.channelResolver(),
                executorService(
                        NUMBER_OF_SCHECHULE_CONTROLLER_THREADS,
                        "ScheduleBootstrapController"
                ),
                scheduleBootstrapper()
        );
    }

    @Bean
    public ScheduleBootstrapper scheduleBootstrapper() {
        return new ScheduleBootstrapper(
                executorService(NUMBER_OF_SCHEDULE_BOOTSTRAP_THREADS, "ScheduleBootstrapper"),
                workers.scheduleBootstrapTaskFactory(),
                workers.scheduleBootstrapWithContentMigrationTaskFactory(),
                workers.equivalenceWritingChannelIntervalScheduleBootstrapTaskFactory()
        );
    }

    @Bean
    public OrganisationBoostrapController organisationBootstrapController() {
        return new OrganisationBoostrapController(
                persistence.legacyOrganisationResolver(),
                persistence.idSettingOrganisationStore()
        );
    }

    private ListeningExecutorService executorService(Integer concurrencyLevel, String namePrefix) {
        return MoreExecutors.listeningDecorator(
                new ThreadPoolExecutor(
                        concurrencyLevel,
                        concurrencyLevel,
                        0, TimeUnit.MICROSECONDS,
                        new ArrayBlockingQueue<Runnable>(100 * Runtime.getRuntime()
                                .availableProcessors()),
                        new ThreadFactoryBuilder().setNameFormat(namePrefix + " Thread %d").build(),
                        new ThreadPoolExecutor.CallerRunsPolicy()
                )
        );
    }
}
