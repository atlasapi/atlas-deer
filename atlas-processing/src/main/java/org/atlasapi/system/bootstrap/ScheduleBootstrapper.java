package org.atlasapi.system.bootstrap;

import com.codepoetics.protonpack.StreamUtils;
import com.google.api.client.util.Sets;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.Channel;
import org.atlasapi.entity.Id;
import org.atlasapi.locks.ScheduleBootstrapLock;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleBootstrapper {

    private static final Logger log = LoggerFactory.getLogger(ScheduleBootstrapper.class);
    private static final long DEFAULT_LOCK_TIMEOUT = 60;
    private static final TimeUnit DEFAULT_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private final Set<Status> bootstrappingStatuses = ConcurrentHashMap.newKeySet();
    private final ScheduleBootstrapLock bootstrapLock = new ScheduleBootstrapLock();
    private final ListeningExecutorService executor;
    private final ChannelIntervalScheduleBootstrapTaskFactory taskFactory;
    private final ScheduleBootstrapWithContentMigrationTaskFactory bootstrapWithMigrationTaskFactory;
    private final EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory equivTaskFactory;
    private final ChannelIntervalScheduleBootstrapTaskFactory forwardingBootstrapTaskFactory;
    private final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

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
            boolean forwarding
    ) {
        Status status = new Status(StreamUtils.stream(channels)
                    .map(Channel::getId)
                    .map(Id::toBigInteger)
                    .map(codec::encode)
                    .collect(MoreCollectors.toImmutableList()),
                    source.key(),
                interval,
                Iterables.size(channels));
        bootstrappingStatuses.add(status);
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
            bootstrappingStatuses.remove(status);
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
                if(!bootstrapLock.tryLock(channel, source.key(), interval, DEFAULT_LOCK_TIMEOUT, DEFAULT_TIMEOUT_UNIT)) {
                    throw new TimeoutException(String.format("Failed to acquire lock for %s %s %s",
                            codec.encode(channel.getId().toBigInteger()),
                            source.key(),
                            interval.toString()));
                }
                try {
                    UpdateProgress progress = task.call();
                    return progress;
                } finally {
                    bootstrapLock.unlock(channel, source.key(), interval);
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
                        status.getTotal(),
                        status.processed.incrementAndGet(),
                        status.failures.get()

                );
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Error while processing schedules for channel "
                                + channel.getId() + "/" + channel.getTitle()
                                + ", bootstrap progress: "
                                + status.progress.incrementAndGet() + "/" + status.getTotal()
                                + ", success: " + status.processed.get()
                                + ", failure: " + status.failures.incrementAndGet(),
                        t
                );
                status.addError(t);
            }
        });
        return updateFuture;
    }

    public Collection<Status> getProgress() {
        return Collections.unmodifiableCollection(bootstrappingStatuses);
    }

    public class Status {
        private final List<String> channels;
        private final String source;
        private final Interval interval;
        private final int total;
        //these are updated on Future callbacks
        private AtomicInteger processed = new AtomicInteger(0);
        private AtomicInteger failures = new AtomicInteger(0);
        private AtomicInteger progress = new AtomicInteger(0);
        private Set<Throwable> errors = ConcurrentHashMap.newKeySet();

        public Status(List<String> channels, String source, Interval interval, int total) {
            this.channels = checkNotNull(channels);
            this.source = checkNotNull(source);
            this.interval = checkNotNull(interval);
            this.total = total;
        }

        public List<String> getChannels() {
            return channels;
        }

        public String getSource() {
            return source;
        }

        public String getInterval() {
            return interval.toString();
        }

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
            return total;
        }

        public void addError(Throwable t) {
            errors.add(t);
        }

        public Set<Throwable> getErrors() {
            return Collections.unmodifiableSet(errors);
        }
    }

}
