package org.atlasapi.query.v4.schedule;

import java.io.IOException;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleEntryListWriter implements EntityListWriter<ItemAndBroadcast> {

    private EntityWriter<Content> contentWriter;
    private EntityWriter<Broadcast> broadcastWriter;

    public ScheduleEntryListWriter(EntityWriter<Content> contentWriter,
            EntityWriter<Broadcast> broadcastWriter) {
        this.contentWriter = checkNotNull(contentWriter);
        this.broadcastWriter = checkNotNull(broadcastWriter);
    }

    @Override
    public void write(ItemAndBroadcast entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeObject(broadcastWriter, "broadcast", entity.getBroadcast(), ctxt);
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
