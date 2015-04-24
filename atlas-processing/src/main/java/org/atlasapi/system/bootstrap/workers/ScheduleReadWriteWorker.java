package org.atlasapi.system.bootstrap.workers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.system.bootstrap.ChannelIntervalScheduleBootstrapTask;
import org.atlasapi.system.bootstrap.SourceChannelIntervalFactory;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.scheduling.UpdateProgress;

public class ScheduleReadWriteWorker implements Worker<ScheduleUpdateMessage> {

    private final Logger log = LoggerFactory.getLogger(ScheduleReadWriteWorker.class);

    private final SubstitutionTableNumberCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final SourceChannelIntervalFactory<ChannelIntervalScheduleBootstrapTask> taskFactory;
    private final ChannelResolver channelResolver;
    private final Set<Publisher> ignoredSources;

    public ScheduleReadWriteWorker(SourceChannelIntervalFactory<ChannelIntervalScheduleBootstrapTask> taskFactory, 
            ChannelResolver channelResolver, Iterable<Publisher> ignoredSources) {
        this.channelResolver = checkNotNull(channelResolver);
        this.taskFactory = checkNotNull(taskFactory);
        this.ignoredSources = ImmutableSet.copyOf(ignoredSources);
    }
    
    @Override
    public void process(ScheduleUpdateMessage msg) {
        String updateMsg = String.format("update %s %s %s-%s", 
                msg.getSource(), msg.getChannel(), msg.getUpdateStart(), msg.getUpdateEnd());
        
        Maybe<Publisher> source = Publisher.fromKey(msg.getSource());
        if (!source.hasValue()) {
            log.warn("{}: unknown source {}", updateMsg, msg.getSource());
            return;
        }
        Publisher src = source.requireValue();
        if (ignoredSources.contains(src)) {
            log.debug("{}: ignoring source {}", updateMsg, src.key());
            return;
        }
        
        long cid = idCodec.decode(msg.getChannel()).longValue();
        Maybe<Channel> channel = channelResolver.fromId(cid);
        if (!channel.hasValue()) {
            log.warn("{}: unknown channel {} ({})", updateMsg, msg.getChannel(), cid);
        }
        
        Interval interval = new Interval(msg.getUpdateStart(), msg.getUpdateEnd());
        
        log.debug("{}: processing", updateMsg);
        try {
            UpdateProgress result = taskFactory.create(src, channel.requireValue(), interval).call();
            log.debug("{}: processed: {}", updateMsg, result);
        } catch (Exception e) {
            log.error("failed " + updateMsg, e);
        }
    }

}
