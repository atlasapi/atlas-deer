package org.atlasapi.query.v4.schedule;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelEquivRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleEntryListWriter implements EntityListWriter<ItemAndBroadcast> {

    private static final Logger log = LoggerFactory.getLogger(ScheduleEntryListWriter.class);

    private final ChannelResolver channelResolver;
    private EntityWriter<ResolvedContent> contentWriter;
    private EntityWriter<ResolvedBroadcast> broadcastWriter;

    public ScheduleEntryListWriter(
            EntityWriter<ResolvedContent> contentWriter,
            EntityWriter<ResolvedBroadcast> broadcastWriter,
            ChannelResolver channelResolver
    ) {
        this.contentWriter = checkNotNull(contentWriter);
        this.broadcastWriter = checkNotNull(broadcastWriter);
        this.channelResolver = checkNotNull(channelResolver);

    }

    @Override
    public void write(ItemAndBroadcast entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {

        ResolvedBroadcast broadcast = ResolvedBroadcast.create(
                entity.getBroadcast(),
                resolveChannel(entity.getBroadcast())
        );

        writer.writeObject(broadcastWriter, "broadcast", broadcast, ctxt);
        writer.writeObject(
                contentWriter,
                "item",
                ResolvedContent.resolvedContentBuilder().withContent(entity.getItem()).build(),
                ctxt
        );
    }

    @Override
    public String fieldName(ItemAndBroadcast entity) {
        return "entry";
    }

    @Override
    public String listName() {
        return "entries";
    }

    //TODO: Move resolution logic to query executor
    private ResolvedChannel resolveChannel(Broadcast broadcast) {

        try {
            Channel channel = Futures.getChecked(
                    channelResolver.resolveIds(
                            ImmutableList.of(broadcast.getChannelId())
                    ),
                    IOException.class
            )
                    .getResources()
                    .first()
                    .orNull();

            return ResolvedChannel.builder()
                    .withChannel(channel)
                    .withResolvedEquivalents(resolveEquivalents(channel.getSameAs()))
                    .build();

        } catch (IOException e) {
            log.error("Failed to resolve channel: {}", broadcast.getChannelId(), e);
            return null;
        }

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
