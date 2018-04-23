package org.atlasapi.system.bootstrap;

import com.google.api.client.util.Sets;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.metabroadcast.common.scheduling.UpdateProgress;
import org.atlasapi.channel.Channel;
import org.atlasapi.locks.ScheduleBootstrapLock;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleBootstrapper {

    private static final Logger log = LoggerFactory.getLogger(ScheduleBootstrapper.class);
    private final ConcurrentHashMap<String, Status> bootstrappingStatuses = new ConcurrentHashMap<>();
    private final ScheduleBootstrapLock bootstrapLock = new ScheduleBootstrapLock();
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

    public Status bootstrapSchedules(
            Iterable<Channel> channels,
            Interval interval,
            Publisher source,
            boolean migrateContent,
            boolean writeEquivalences,
            boolean forwarding,
            String bootstrapName
    ) {
        Status status = new Status();
        status.total.set(Iterables.size(channels));
        bootstrappingStatuses.put(bootstrapName, status);
        Set<ListenableFuture<UpdateProgress>> futures = Sets.newHashSet();
        log.info(
                "Bootstrapping {} channels for interval from {} to {}",
                Iterables.size(channels),
                interval.getStart(),
                interval.getEnd()
        );
        for(Channel channel : channels) {
                futures.add(bootstrapChannel(
                        channel,
                        interval,
                        source,
                        migrateContent,
                        writeEquivalences,
                        forwarding,
                        status
                ));
        }
        try {
            Futures.getChecked(Futures.allAsList(futures), Exception.class);
        } catch (Exception e) {
            //this is already logged in the callback
        } finally {
            bootstrappingStatuses.remove(bootstrapName, status);
        }
        return status;
    }

    private ListenableFuture<UpdateProgress> bootstrapChannel(
            final Channel channel,
            Interval interval,
            Publisher source,
            boolean migrateContent,
            boolean writeEquivalences,
            boolean forwarding,
            Status status
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
        ListenableFuture<UpdateProgress> updateFuture = executor.submit(new Callable<UpdateProgress>() {
            @Override
            public UpdateProgress call() throws Exception {
                bootstrapLock.lock(channel, interval);
                try {
                    UpdateProgress progress = task.call();
                    bootstrapLock.unlock(channel, interval);
                    return progress;
                } catch(Exception e) {
                    bootstrapLock.unlock(channel, interval);
                    throw e;
                }
        }});
        Futures.addCallback(updateFuture, new FutureCallback<UpdateProgress>() {

            @Override
            public void onSuccess(UpdateProgress result) {
                log.info(
                        "Processed channel {}/{} with result: ({}), bootstrap progress: {}/{}, success: {}, failure: {}",
                        channel.getId(),
                        channel.getTitle(),
                        result,
                        status.progress.incrementAndGet(),
                        status.total.get(),
                        status.processed.incrementAndGet(),
                        status.failures.get()

                );
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Error while processing schedules for channel "
                                + channel.getId() + "/" + channel.getTitle()
                                + ", bootstrap progress: "
                                + status.progress.incrementAndGet() + "/" + status.total.get()
                                + ", success: " + status.processed.get()
                                + ", failure: " + status.failures.incrementAndGet(),
                        t
                );
            }
        });
        return updateFuture;
    }

    public Optional<Status> getProgress(String bootstrapName) {
        return Optional.ofNullable(bootstrappingStatuses.get(bootstrapName));
    }

    public Map<String, Status> getProgress() {
        return Collections.unmodifiableMap(bootstrappingStatuses);
    }

    public boolean isBootstrapping() {
        return !bootstrappingStatuses.isEmpty();
    }

    public class Status {
        private AtomicInteger processed = new AtomicInteger(0);
        private AtomicInteger failures = new AtomicInteger(0);
        private AtomicInteger progress = new AtomicInteger(0);
        private AtomicInteger total = new AtomicInteger(0);

        public int getProcessed() {
            return processed.get();
        }

        public int getFailures() {
            return failures.get();
        }

        public int getProgress() {
            return progress.get();
        }

        public int getTotal() {
            return total.get();
        }
    }

}
