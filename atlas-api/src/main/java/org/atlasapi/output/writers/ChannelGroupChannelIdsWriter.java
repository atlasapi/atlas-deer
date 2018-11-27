package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelIdWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupChannelIdsWriter implements EntityListWriter<ChannelGroupMembership> {

    private final ChannelIdWriter channelIdWriter;

    public ChannelGroupChannelIdsWriter(ChannelIdWriter channelIdWriter) {
        this.channelIdWriter = checkNotNull(channelIdWriter);
    }

    @Override
    public String listName() {
        return "channels";
    }

    @Override
    public void write(
            @Nonnull ChannelGroupMembership entity,
            @Nonnull FieldWriter format,
            @Nonnull OutputContext ctxt
    ) throws IOException {
        format.writeObject(channelIdWriter, "channel", entity, ctxt);
    }

    @Nonnull
    @Override
    public String fieldName(ChannelGroupMembership entity) {
        return "channel";
    }
}
