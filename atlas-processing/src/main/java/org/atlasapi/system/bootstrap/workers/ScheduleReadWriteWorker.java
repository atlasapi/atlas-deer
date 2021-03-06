package org.atlasapi.system.bootstrap.workers;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.system.bootstrap.ChannelIntervalScheduleBootstrapTask;
import org.atlasapi.system.bootstrap.SourceChannelIntervalFactory;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleReadWriteWorker implements Worker<ScheduleUpdateMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduleReadWriteWorker.class);

    private final SubstitutionTableNumberCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final SourceChannelIntervalFactory<ChannelIntervalScheduleBootstrapTask> taskFactory;
    private final ChannelResolver channelResolver;
    private final Set<Publisher> ignoredSources;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;
    private final String publisherExecutionTimerName;
    private final String publisherMeterName;
    private final String publisherLatencyTimerName;

    private final MetricRegistry metricRegistry;
    @Nullable private final RateLimiter rateLimiter;

    private ScheduleReadWriteWorker(
            SourceChannelIntervalFactory<ChannelIntervalScheduleBootstrapTask> taskFactory,
            ChannelResolver channelResolver,
            Iterable<Publisher> ignoredSources,
            String metricPrefix,
            MetricRegistry metricRegistry,
            @Nullable RateLimiter rateLimiter
    ) {
        this.channelResolver = checkNotNull(channelResolver);
        this.taskFactory = checkNotNull(taskFactory);
        this.ignoredSources = ImmutableSet.copyOf(ignoredSources);

        this.metricRegistry = metricRegistry;

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
        this.publisherMeterName = metricPrefix + "source.%s.meter.received";
        this.publisherExecutionTimerName = metricPrefix + "source.%s.timer.execution";
        this.publisherLatencyTimerName = metricPrefix + "source.%s.timer.latency";
        this.rateLimiter = rateLimiter;
        if (this.rateLimiter != null) {
            LOG.info("Limiting rate to a maximum of {} messages per second", this.rateLimiter.getRate());
        }
    }

    public static ScheduleReadWriteWorker create(
            SourceChannelIntervalFactory<ChannelIntervalScheduleBootstrapTask> taskFactory,
            ChannelResolver channelResolver,
            Iterable<Publisher> ignoredSources,
            String metricPrefix,
            MetricRegistry metricRegistry,
            @Nullable RateLimiter rateLimiter
    ) {
        return new ScheduleReadWriteWorker(
                taskFactory,
                channelResolver,
                ignoredSources,
                metricPrefix,
                metricRegistry,
                rateLimiter
        );
    }

    @Override
    public void process(ScheduleUpdateMessage msg) {
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        long start = System.currentTimeMillis();

        messageReceivedMeter.mark();

        LOG.debug(
                "Processing message on id {}, took: PT{}S, from: {}, to: {}",
                msg.getChannel(),
                getTimeToProcessInMillis(msg.getTimestamp()) / 1000L,
                msg.getUpdateStart(),
                msg.getUpdateEnd()
        );

        String updateMsg = String.format("update %s %s %s-%s",
                msg.getSource(), msg.getChannel(), msg.getUpdateStart(), msg.getUpdateEnd()
        );

        Maybe<Publisher> source = Publisher.fromKey(msg.getSource());
        if (!source.hasValue()) {
            LOG.warn("{}: unknown source {}", updateMsg, msg.getSource());
            return;
        }
        Publisher src = source.requireValue();
        if (ignoredSources.contains(src)) {
            LOG.debug("{}: ignoring source {}", updateMsg, src.key());
            return;
        }

        String metricSourceName = src.key().replace('.', '_');

        metricRegistry.meter(String.format(publisherMeterName, metricSourceName)).mark();

        Timer publisherExecutionTimer = metricRegistry.timer(String.format(publisherExecutionTimerName, metricSourceName));
        Timer publisherLatencyTimer = metricRegistry.timer(String.format(publisherLatencyTimerName, metricSourceName));

        Id cid = Id.valueOf(idCodec.decode(msg.getChannel()));

        LOG.debug("Processing message on id {}, message: {}", cid, msg);

        Timer.Context time = executionTimer.time();
        Timer.Context publisherTime = publisherExecutionTimer.time();

        try {
            ListenableFuture<Resolved<Channel>> channelFuture = channelResolver.resolveIds(
                    ImmutableList.of(cid)
            );

            Resolved<Channel> resolvedChannel = Futures.get(
                    channelFuture,
                    1,
                    TimeUnit.MINUTES,
                    Exception.class
            );

            if (resolvedChannel.getResources().isEmpty()) {
                LOG.warn("{}: unknown channel {} ({})", updateMsg, msg.getChannel(), cid);
            }
            Interval interval = new Interval(msg.getUpdateStart(), msg.getUpdateEnd());
            UpdateProgress result = taskFactory.create(
                    src,
                    Iterables.getOnlyElement(resolvedChannel.getResources()),
                    interval
            ).call();
            LOG.debug("{}: processed: {}", updateMsg, result);
            if (Publisher.BBC_NITRO.equals(src) || Publisher.BT_BLACKOUT.equals(src)) {
                updatePaSchedule(updateMsg, resolvedChannel, interval);
            }

            long timeToProcessInMillis = getTimeToProcessInMillis(msg.getTimestamp());

            latencyTimer.update(timeToProcessInMillis, TimeUnit.MILLISECONDS);
            publisherLatencyTimer.update(timeToProcessInMillis, TimeUnit.MILLISECONDS);

            long end = System.currentTimeMillis();
            LOG.info(
                    "Timings - Source: {}, Execution Time (ms): {}, Latency (ms): {}",
                    src.key(),
                    end - start,
                    timeToProcessInMillis
            );
        } catch (Exception e) {
            failureMeter.mark();
            LOG.error("failed " + updateMsg, e);

            throw Throwables.propagate(e);
        } finally {
            time.stop();
            publisherTime.stop();
        }
    }

    /**
     * Force an update of the PA schedule. This is ahead of full equivalent schedule store
     * maintenance
     */
    private void updatePaSchedule(String updateMsg, Resolved<Channel> resolvedChannel,
            Interval interval) throws Exception {
        UpdateProgress paResult = taskFactory.create(
                Publisher.PA,
                Iterables.getOnlyElement(resolvedChannel.getResources()),
                interval
        ).call();
        LOG.debug("{}: processed: {}", updateMsg, paResult);
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
