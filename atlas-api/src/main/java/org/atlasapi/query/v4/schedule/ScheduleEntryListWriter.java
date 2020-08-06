package org.atlasapi.query.v4.schedule;

import org.atlasapi.content.Content;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResolvedChannelResolver;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleEntryListWriter implements EntityListWriter<ItemAndBroadcast> {

    private final ResolvedChannelResolver resolvedChannelResolver;
    private EntityWriter<Content> contentWriter;
    private EntityWriter<ResolvedBroadcast> broadcastWriter;

    public ScheduleEntryListWriter(
            EntityWriter<Content> contentWriter,
            EntityWriter<ResolvedBroadcast> broadcastWriter,
            ResolvedChannelResolver resolvedChannelResolver
    ) {
        this.contentWriter = checkNotNull(contentWriter);
        this.broadcastWriter = checkNotNull(broadcastWriter);
        this.resolvedChannelResolver = checkNotNull(resolvedChannelResolver);

    }

    @Override
    public void write(ItemAndBroadcast entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {

        ResolvedBroadcast broadcast = ResolvedBroadcast.create(
                entity.getBroadcast(),
                resolvedChannelResolver.resolveChannel(entity.getBroadcast())
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
}
