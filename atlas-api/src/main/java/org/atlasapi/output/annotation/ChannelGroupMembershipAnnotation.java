package org.atlasapi.output.annotation;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.channel.Channel;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupMembershipAnnotation extends OutputAnnotation<Channel> {

    private final NumberToShortStringCodec idCodec;

    public ChannelGroupMembershipAnnotation(NumberToShortStringCodec idCodec) {
        this.idCodec = checkNotNull(idCodec);
    }


    @Override
    public void write(Channel entity, FieldWriter format, OutputContext ctxt) throws IOException {
        format.writeList(new ChannelGroupMembershipListWriter("channel_groups", "channel_group", idCodec), entity.getChannelGroups(), ctxt);

    }
}
