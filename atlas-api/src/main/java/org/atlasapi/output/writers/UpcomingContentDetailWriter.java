package org.atlasapi.output.writers;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelEquivRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class UpcomingContentDetailWriter implements EntityListWriter<ResolvedContent> {

    private static final Logger log = LoggerFactory.getLogger(UpcomingContentDetailWriter.class);

    private final BroadcastWriter broadcastWriter;
    private final ItemDetailWriter itemDetailWriter;
    private final ChannelResolver channelResolver;

    public UpcomingContentDetailWriter(
            BroadcastWriter broadcastWriter,
            ItemDetailWriter itemDetailWriter,
            ChannelResolver channelResolver
    ) {
        this.broadcastWriter = checkNotNull(broadcastWriter);
        this.itemDetailWriter = checkNotNull(itemDetailWriter);
        this.channelResolver = checkNotNull(channelResolver);
    }

    @Override
    public String listName() {
        return "upcoming_content_detail";
    }

    @Override
    public void write(@Nonnull ResolvedContent entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        if (entity.getContent() instanceof Item) {
            Item item = (Item) entity.getContent();
            List<ResolvedBroadcast> sortedBroadcasts = item.getBroadcasts()
                    .stream()
                    .sorted(Broadcast.startTimeOrdering())
                    .map(broadcast -> ResolvedBroadcast.create(broadcast, resolveChannel(broadcast)))
                    .collect(Collectors.toList());

            writer.writeObject(broadcastWriter, Iterables.getFirst(sortedBroadcasts, null), ctxt);
            writer.writeObject(itemDetailWriter, entity, ctxt);
        }
    }

    @Nonnull
    @Override
    public String fieldName(ResolvedContent entity) {
        return null;
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
            log.error("Failed to resolveContent channel: {}", broadcast.getChannelId(), e);
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
            log.error("Failed to resolveContent channel equivlents", e);
            return null;
        }
    }
}
