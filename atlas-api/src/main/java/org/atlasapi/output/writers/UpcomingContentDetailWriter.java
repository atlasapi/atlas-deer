package org.atlasapi.output.writers;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class UpcomingContentDetailWriter implements EntityListWriter<Item> {

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
    public void write(@Nonnull Item entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        List<ResolvedBroadcast> sortedBroadcasts = entity.getBroadcasts()
                .stream()
                .sorted(Broadcast.startTimeOrdering())
                .map(broadcast -> ResolvedBroadcast.create(broadcast, resolveChannel(broadcast)))
                .collect(Collectors.toList());

        writer.writeObject(broadcastWriter, Iterables.getFirst(sortedBroadcasts, null), ctxt);
        writer.writeObject(itemDetailWriter, entity, ctxt);
    }

    @Nonnull
    @Override
    public String fieldName(Item entity) {
        return null;
    }

    private ResolvedChannel resolveChannel(Broadcast broadcast) {

        try {
            return ResolvedChannel.builder(
                    Futures.getChecked(
                        channelResolver.resolveIds(
                                ImmutableList.of(broadcast.getChannelId())
                        ),
                        IOException.class
                    )
                        .getResources()
                        .first()
                        .orNull()
            )
                    .build();

        } catch (IOException e) {
            log.error("Failed to resolve channel: {}", broadcast.getChannelId(), e);
            return null;
        }

    }
}
