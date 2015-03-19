package org.atlasapi.query.v4.channelgroup;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.output.ChannelWithChannelGroupMembership;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelWriter;

import javax.annotation.Nonnull;
import java.io.IOException;

public class ChannelGroupChannelWriter implements EntityListWriter<ChannelWithChannelGroupMembership> {

    private static final ChannelWriter CHANNEL_WRITER = new ChannelWriter("channels", "channel");

    @Override
    public String listName() {
        return "channels";
    }

    @Override
    public void write(@Nonnull ChannelWithChannelGroupMembership entity, @Nonnull FieldWriter format, @Nonnull OutputContext ctxt) throws IOException {
        Channel channel = entity.getChannel();
        ChannelGroupMembership channelGroupMembership = entity.getChannelGroupMembership();

        format.writeObject(CHANNEL_WRITER, "channel", channel, ctxt);
        if (channelGroupMembership instanceof ChannelNumbering) {
            ChannelNumbering channelNumbering = ((ChannelNumbering) channelGroupMembership);
            format.writeField("channel_number",channelNumbering.getChannelNumber());
            format.writeField("start_date", channelNumbering.getStartDate());
        }

    }

    @Nonnull
    @Override
    public String fieldName(ChannelWithChannelGroupMembership entity) {
        return "channel";
    }
}
