package org.atlasapi.query.v4.schedule;

import java.io.IOException;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.schedule.ChannelSchedule;

public class ScheduleListWriter implements EntityListWriter<ChannelSchedule> {

    private final EntityWriter<ResolvedChannel> channelWriter;
    private final EntityListWriter<ItemAndBroadcast> entryWriter;

    public ScheduleListWriter(EntityWriter<ResolvedChannel> channelWriter,
            EntityListWriter<ItemAndBroadcast> contentWriter) {
        this.channelWriter = channelWriter;
        this.entryWriter = contentWriter;
    }

    @Override
    public void write(ChannelSchedule entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        ResolvedChannel resolvedChannel = ResolvedChannel.builder(entity.getChannel()).build();
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

}
