package org.atlasapi.output.annotation;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import java.io.IOException;
import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.output.writers.SourceWriter.sourceListWriter;

public class ChannelGroupMembershipListWriter implements EntityListWriter<ChannelGroupMembership> {

    private final String listName;
    private final String fieldName;
    private final NumberToShortStringCodec idCodec;
    private static final EntityListWriter<Publisher> PUBLISHER_WRITER = sourceListWriter("publisher");


    public ChannelGroupMembershipListWriter(
            String listName,
            String fieldName,
            NumberToShortStringCodec idCodec
    ) {
        this.listName = checkNotNull(listName);
        this.fieldName = checkNotNull(fieldName);
        this.idCodec = checkNotNull(idCodec);
    }


    @Override
    public String listName() {
        return listName;
    }

    @Override
    public void write(ChannelGroupMembership entity,FieldWriter writer, OutputContext ctxt) throws IOException {
        BigInteger channelId = entity.getChannel().getId().toBigInteger();
        BigInteger channelGroupId = entity.getChannelGroup().getId().toBigInteger();
        writer.writeField("channel_id", idCodec.encode(channelId));
        writer.writeField("channel_group_id", idCodec.encode(channelGroupId));
        if (entity instanceof ChannelNumbering) {
            writer.writeField(
                    "channel_number",
                    ((ChannelNumbering) entity).getChannelNumber()
            );
        }
        writer.writeObject(PUBLISHER_WRITER, entity.getChannel().getPublisher(), ctxt);

    }

    @Override
    public String fieldName(ChannelGroupMembership entity) {
        return fieldName;
    }
}
