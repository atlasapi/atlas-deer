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
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleBootstrapper {

    private static final Logger log = LoggerFactory.getLogger(ScheduleBootstrapper.class);
    private final ReentrantLock bootstrapLock = new ReentrantLock();
    private final AtomicBoolean bootstrapping = new AtomicBoolean(false);
    private final AtomicInteger processed = new AtomicInteger(0);
    private final AtomicInteger failures = new AtomicInteger(0);
    private final ListeningExecutorService executor;
    private final ChannelIntervalScheduleBootstrapTaskFactory taskFactory;

    public ScheduleBootstrapper(
            ListeningExecutorService executor,
            ChannelIntervalScheduleBootstrapTaskFactory taskFactory
    ) {
        this.executor = checkNotNull(executor);
        this.taskFactory = checkNotNull(taskFactory);
    }


    public boolean bootstrapSchedules(Iterable<Channel> channels, Interval interval, Publisher source) {
        if (!bootstrapLock.tryLock()) {
            return false;
        }
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
                futures.add(bootstrapChannel(channel, interval, source));
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

    private ListenableFuture<UpdateProgress> bootstrapChannel(final Channel channel, Interval interval, Publisher source) {
        log.info("Bootstrapping channel {}/{}", channel.getId(), channel.getTitle());
        ListenableFuture<UpdateProgress> updateFuture = executor.submit(
                taskFactory.create(source, channel, interval)
        );
        Futures.addCallback(updateFuture, new FutureCallback<UpdateProgress>() {
            @Override
            public void onSuccess(UpdateProgress result) {
                processed.incrementAndGet();
                log.info("Processed channel {} with updates {}", channel.getId(), result);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Error while processing schedules", t);
                failures.incrementAndGet();
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
