package org.atlasapi.system.bootstrap;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.atlasapi.channel.Channel;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.scheduling.UpdateProgress;

import com.google.api.client.util.Sets;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleBootstrapper {

    private static final Logger log = LoggerFactory.getLogger(ScheduleBootstrapper.class);
    private final ReentrantLock bootstrapLock = new ReentrantLock();
    private final AtomicBoolean bootstrapping = new AtomicBoolean(false);
    private final AtomicInteger processed = new AtomicInteger(0);
    private final AtomicInteger failures = new AtomicInteger(0);
    private final AtomicInteger progress = new AtomicInteger(0);
    private final AtomicInteger total = new AtomicInteger(0);
    private final ListeningExecutorService executor;
    private final ChannelIntervalScheduleBootstrapTaskFactory taskFactory;
    private final ScheduleBootstrapWithContentMigrationTaskFactory bootstrapWithMigrationTaskFactory;
    private final EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory equivTaskFactory;
    private final ChannelIntervalScheduleBootstrapTaskFactory forwardingBootstrapTaskFactory;

    public ScheduleBootstrapper(
            ListeningExecutorService executor,
            ChannelIntervalScheduleBootstrapTaskFactory taskFactory,
            ScheduleBootstrapWithContentMigrationTaskFactory bootstrapWithMigrationTaskFactory,
            EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory bootstrapWithEquiv,
            ChannelIntervalScheduleBootstrapTaskFactory forwardingBootstrapTaskFactory
    ) {
        this.executor = checkNotNull(executor);
        this.taskFactory = checkNotNull(taskFactory);
        this.bootstrapWithMigrationTaskFactory = checkNotNull(bootstrapWithMigrationTaskFactory);
        this.equivTaskFactory = checkNotNull(bootstrapWithEquiv);
        this.forwardingBootstrapTaskFactory = checkNotNull(forwardingBootstrapTaskFactory);
    }

    public boolean bootstrapSchedules(
            Iterable<Channel> channels,
            Interval interval,
            Publisher source,
            boolean migrateContent,
            boolean writeEquivalences,
            boolean forwarding
    ) {
        if (!bootstrapLock.tryLock()) {
            return false;
        }
        bootstrapping.set(true);
        processed.set(0);
        failures.set(0);
        progress.set(0);
        total.set(Iterables.size(channels));
        Set<ListenableFuture<UpdateProgress>> futures = Sets.newHashSet();
        log.info(
                "Bootstrapping {} channels for interval from {} to {}",
                Iterables.size(channels),
                interval.getStart(),
                interval.getEnd()
        );
        try {
            for (Channel channel : channels) {
                futures.add(bootstrapChannel(
                        channel,
                        interval,
                        source,
                        migrateContent,
                        writeEquivalences,
                        forwarding
                ));
            }
            Futures.get(Futures.allAsList(futures), Exception.class);
        } catch (Exception e) {
            //this is already logged in the callback
        } finally {
            bootstrapLock.unlock();
            bootstrapping.set(false);
        }
        return true;
    }

    public boolean bootstrapSchedules(Iterable<Channel> channels, Interval interval,
            Publisher source, boolean migrateContent) {
        return bootstrapSchedules(channels, interval, source, migrateContent, false, false);
    }

    private ListenableFuture<UpdateProgress> bootstrapChannel(
            final Channel channel,
            Interval interval,
            Publisher source,
            boolean migrateContent,
            boolean writeEquivalences,
            boolean forwarding
    ) {
        log.info("Bootstrapping channel {}/{}", channel.getId(), channel.getTitle());
        ChannelIntervalScheduleBootstrapTask task;
        if (migrateContent) {
            task = bootstrapWithMigrationTaskFactory.create(source, channel, interval);
        } else if (writeEquivalences) {
            task = equivTaskFactory.create(source, channel, interval);
        } else if (forwarding) {
            task = forwardingBootstrapTaskFactory.create(source, channel, interval);
        } else {
            task = taskFactory.create(source, channel, interval);
        }
        ListenableFuture<UpdateProgress> updateFuture = executor.submit(task);
        Futures.addCallback(updateFuture, new FutureCallback<UpdateProgress>() {

            @Override
            public void onSuccess(UpdateProgress result) {
                log.info(
                        "Processed channel {}/{} with result: ({}), bootstrap progress: {}/{}, success: {}, failure: {}",
                        channel.getId(),
                        channel.getTitle(),
                        result,
                        progress.incrementAndGet(),
                        total.get(),
                        processed.incrementAndGet(),
                        failures.get()

                );
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Error while processing schedules for channel "
                                + channel.getId() + "/" + channel.getTitle()
                                + ", bootstrap progress: "
                                + progress.incrementAndGet() + "/" + total.get()
                                + ", success: " + processed.get()
                                + ", failure: " + failures.incrementAndGet(),
                        t
                );
            }
        });
        return updateFuture;
    }

    public UpdateProgress getProgress() {
        return new UpdateProgress(processed.get(), failures.get());
    }

    public boolean isBootstrapping() {
        return bootstrapping.get();
    }

}
