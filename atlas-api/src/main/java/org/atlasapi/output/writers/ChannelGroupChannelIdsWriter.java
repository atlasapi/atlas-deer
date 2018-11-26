package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelIdWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupChannelIdsWriter implements EntityListWriter<ResolvedChannel> {

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
            @Nonnull ResolvedChannel entity,
            @Nonnull FieldWriter format,
            @Nonnull OutputContext ctxt
    ) throws IOException {
        format.writeObject(channelIdWriter, "channel", entity, ctxt);
    }

    @Nonnull
    @Override
    public String fieldName(ResolvedChannel entity) {
        return "channel";
    }
}
