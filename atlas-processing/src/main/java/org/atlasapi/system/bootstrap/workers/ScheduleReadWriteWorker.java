package org.atlasapi.system.bootstrap.workers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.concurrent.TimeUnit;

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

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.scheduling.UpdateProgress;

public class ScheduleReadWriteWorker implements Worker<ScheduleUpdateMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduleReadWriteWorker.class);

    private final SubstitutionTableNumberCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final SourceChannelIntervalFactory<ChannelIntervalScheduleBootstrapTask> taskFactory;
    private final ChannelResolver channelResolver;
    private final Set<Publisher> ignoredSources;
    private final Timer metricsTimer;

    public ScheduleReadWriteWorker(
            SourceChannelIntervalFactory<ChannelIntervalScheduleBootstrapTask> taskFactory,
            ChannelResolver channelResolver,
            Iterable<Publisher> ignoredSources,
            Timer metricsTimer
    ) {
        this.channelResolver = checkNotNull(channelResolver);
        this.taskFactory = checkNotNull(taskFactory);
        this.ignoredSources = ImmutableSet.copyOf(ignoredSources);
        this.metricsTimer = checkNotNull(metricsTimer);
    }
    
    @Override
    public void process(ScheduleUpdateMessage msg) {
        String updateMsg = String.format("update %s %s %s-%s",
                msg.getSource(), msg.getChannel(), msg.getUpdateStart(), msg.getUpdateEnd());
        
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
        
        Id cid = Id.valueOf(idCodec.decode(msg.getChannel()));

        LOG.debug("Processing message on id {}, message: {}", cid, msg);

        Timer.Context time = metricsTimer.time();
        ListenableFuture<Resolved<Channel>> channelFuture = channelResolver.resolveIds(ImmutableList.of(cid));
        try {
            Resolved<Channel> resolvedChannel = Futures.get(channelFuture, 1, TimeUnit.MINUTES, Exception.class);

            if (resolvedChannel.getResources().isEmpty()) {
                LOG.warn("{}: unknown channel {} ({})", updateMsg, msg.getChannel(), cid);
            }
            Interval interval = new Interval(msg.getUpdateStart(), msg.getUpdateEnd());
            UpdateProgress result = taskFactory.create(src, Iterables.getOnlyElement(resolvedChannel.getResources()), interval).call();
            LOG.debug("{}: processed: {}", updateMsg, result);
            if (!Publisher.PA.equals(src)) {
                updatePaSchedule(updateMsg, resolvedChannel, interval);
            }

            time.stop();
        } catch (Exception e) {
            LOG.error("failed " + updateMsg, e);
        }
    }

    /**
     * Force an update of the PA schedule. This is ahead of full equivalent
     * schedule store maintenance
     */
    private void updatePaSchedule(String updateMsg, Resolved<Channel> resolvedChannel,
            Interval interval) throws Exception {
        UpdateProgress paResult = taskFactory.create(Publisher.PA, Iterables.getOnlyElement(resolvedChannel.getResources()), interval).call();
        LOG.debug("{}: processed: {}", updateMsg, paResult);
    }

}
