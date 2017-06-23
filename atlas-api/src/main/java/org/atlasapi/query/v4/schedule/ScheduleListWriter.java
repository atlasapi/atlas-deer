package org.atlasapi.query.v4.schedule;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Iterables;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelEquivRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.schedule.ChannelSchedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class ScheduleListWriter implements EntityListWriter<ChannelSchedule> {

    private static final Logger log = LoggerFactory.getLogger(ScheduleListWriter.class);

    private final EntityWriter<ResolvedChannel> channelWriter;
    private final EntityListWriter<ItemAndBroadcast> entryWriter;
    private final ChannelResolver channelResolver;

    public ScheduleListWriter(
            EntityWriter<ResolvedChannel> channelWriter,
            EntityListWriter<ItemAndBroadcast> contentWriter,
            ChannelResolver channelResolver
    ) {
        this.channelWriter = channelWriter;
        this.entryWriter = contentWriter;
        this.channelResolver = channelResolver;
    }

    @Override
    public void write(ChannelSchedule entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        ResolvedChannel resolvedChannel = resolveChannel(entity.getChannel());
        writer.writeObject(channelWriter, resolvedChannel, ctxt);
        writer.writeField("source", entity.getSource());
        writer.writeList(entryWriter, entity.getEntries(), ctxt);
    }

    @Override
    public String fieldName(ChannelSchedule entity) {
        return "schedule";
    }

    @Override
    public String listName() {
        return "schedules";
    }

    private ResolvedChannel resolveChannel(Channel channel) {
        return ResolvedChannel.builder()
                .withChannel(channel)
                .withResolvedEquivalents(resolveEquivalents(channel.getSameAs()))
                .build();
    }

    @Nullable
    private Iterable<Channel> resolveEquivalents(Set<ChannelEquivRef> channelRefs) {
        try {
            if (channelRefs != null && !channelRefs.isEmpty()) {
                Iterable<Id> ids = Iterables.transform(channelRefs, ResourceRef::getId);
                return channelResolver.resolveIds(ids).get(1, TimeUnit.MINUTES).getResources();
            }

            return null;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to resolve channel equivlents", e);
            return null;
        }
    }
}
