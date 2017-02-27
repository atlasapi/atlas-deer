package org.atlasapi.query.v4.schedule;

import java.io.IOException;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleEntryListWriter implements EntityListWriter<ItemAndBroadcast> {

    private static final Logger log = LoggerFactory.getLogger(ScheduleEntryListWriter.class);

    private final ChannelResolver channelResolver;
    private EntityWriter<Content> contentWriter;
    private EntityWriter<ResolvedBroadcast> broadcastWriter;

    public ScheduleEntryListWriter(
            EntityWriter<Content> contentWriter,
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
        writer.writeObject(contentWriter, "item", entity.getItem(), ctxt);
    }

    @Override
    public String fieldName(ItemAndBroadcast entity) {
        return "entry";
    }

    @Override
    public String listName() {
        return "entries";
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
