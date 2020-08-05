package org.atlasapi.output.writers;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResolvedChannelResolver;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class UpcomingContentDetailWriter implements EntityListWriter<Item> {

    private final BroadcastWriter broadcastWriter;
    private final ItemDetailWriter itemDetailWriter;
    private final ResolvedChannelResolver resolvedChannelResolver;

    public UpcomingContentDetailWriter(
            BroadcastWriter broadcastWriter,
            ItemDetailWriter itemDetailWriter,
            ResolvedChannelResolver resolvedChannelResolver
    ) {
        this.broadcastWriter = checkNotNull(broadcastWriter);
        this.itemDetailWriter = checkNotNull(itemDetailWriter);
        this.resolvedChannelResolver = checkNotNull(resolvedChannelResolver);
    }

    @Override
    public String listName() {
        return "upcoming_content_detail";
    }

    @Override
    public void write(@Nonnull Item entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        List<Broadcast> broadcasts = entity.getBroadcasts()
                .stream()
                .sorted(Broadcast.startTimeOrdering())
                .collect(MoreCollectors.toImmutableList());

        Map<Id, ResolvedChannel> channelMap = resolvedChannelResolver.resolveChannelMap(broadcasts);

        List<ResolvedBroadcast> sortedBroadcasts = broadcasts.stream()
                .map(broadcast -> ResolvedBroadcast.create(broadcast, channelMap.get(broadcast.getChannelId())))
                .collect(Collectors.toList());

        writer.writeObject(broadcastWriter, Iterables.getFirst(sortedBroadcasts, null), ctxt);
        writer.writeObject(itemDetailWriter, entity, ctxt);
    }

    @Nonnull
    @Override
    public String fieldName(Item entity) {
        return null;
    }
}
