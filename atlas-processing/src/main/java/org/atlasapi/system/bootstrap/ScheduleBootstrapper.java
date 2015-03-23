package org.atlasapi.system.bootstrap;

import com.google.api.client.util.Sets;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.metabroadcast.common.scheduling.UpdateProgress;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class ScheduleBootstrapper {

    private static final Logger log = LoggerFactory.getLogger(ScheduleBootstrapper.class);
    private final ReentrantLock bootstrapLock = new ReentrantLock();
    private final AtomicBoolean bootstrapping = new AtomicBoolean(false);
    private final AtomicInteger processed = new AtomicInteger(0);
    private final AtomicInteger failures = new AtomicInteger(0);
    private final AtomicReference<Throwable> lastException = new AtomicReference<>();
    private final ListeningExecutorService executor;
    private final ChannelIntervalScheduleBootstrapTaskFactory taskFactory;

    public ScheduleBootstrapper(
            ListeningExecutorService executor,
            ChannelIntervalScheduleBootstrapTaskFactory taskFactory
    ) {
        this.executor = executor;
        this.taskFactory = taskFactory;
    }


    public boolean bootstrapSchedules(Iterable<Channel> channels, Interval interval, Publisher source) {
        if (bootstrapLock.tryLock()) {
            bootstrapping.set(true);
            processed.set(0);
            failures.set(0);
            Set<ListenableFuture<UpdateProgress>> futures = Sets.newHashSet();
            log.info(
                    "Bootstrapping {} channels for interval from {} to {}",
                    Iterables.size(channels),
                    interval.getStart(),
                    interval.getEnd()
            );
            try {
                for (Channel channel : channels) {
                    log.info("Bootstrapping channel {}/{}", channel.getId(), channel.getTitle());
                    ListenableFuture<UpdateProgress> updateFuture = executor.submit(
                            taskFactory.create(source, channel, interval)
                    );
                    Futures.addCallback(updateFuture, new FutureCallback<UpdateProgress>() {
                        @Override
                        public void onSuccess(UpdateProgress result) {
                            processed.incrementAndGet();
                            log.info("Processed channel with updates {}", result);
                        }
                        @Override
                        public void onFailure(Throwable t) {
                            log.error("Error while processing schedules", t);
                            failures.incrementAndGet();
                            lastException.set(t);
                        }
                    });
                    futures.add(updateFuture);
                }
                Futures.get(Futures.allAsList(futures), Exception.class);

            } catch (Exception e) {
                lastException.set(e);
            } finally {
                bootstrapLock.unlock();
                bootstrapping.set(false);
            }
            return true;
        } else {
            return false;
        }
    }


    public UpdateProgress getProgress() {
        return new UpdateProgress(processed.get(), failures.get());
    }

    public boolean isBootstrapping() {
        return bootstrapping.get();
    }

    public Throwable getLastException() {
        return lastException.get();
    }

}
