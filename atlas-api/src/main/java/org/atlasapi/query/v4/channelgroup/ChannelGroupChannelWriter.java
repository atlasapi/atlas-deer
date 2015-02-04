package org.atlasapi.query.v4.channelgroup;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupChannelWriter implements EntityListWriter<ChannelGroupMembership> {

    private final ChannelResolver channelResolver;
    private static final ChannelWriter CHANNEL_WRITER = new ChannelWriter("channels", "channel");

    public ChannelGroupChannelWriter(ChannelResolver channelResolver) {
        this.channelResolver = checkNotNull(channelResolver);
    }

    @Override
    public String listName() {
        return "channels";
    }

    @Override
    public void write(@Nonnull ChannelGroupMembership entity, @Nonnull FieldWriter format, @Nonnull OutputContext ctxt) throws IOException {
        Channel channel = Futures.get(
                Futures.transform(
                        this.channelResolver.resolveIds(ImmutableSet.of(entity.getChannel().getId())),
                        new Function<Resolved<Channel>, Channel>() {
                            @Nullable
                            @Override
                            public Channel apply(Resolved<Channel> input) {
                                return input.getResources().first().get();
                            }
                        }
                ), 1, TimeUnit.MINUTES, IOException.class
        );
        format.writeObject(CHANNEL_WRITER, "channel", channel, ctxt);
        if (entity instanceof ChannelNumbering) {
            ChannelNumbering channelNumbering = ((ChannelNumbering) entity);
            format.writeField("channel_number",channelNumbering.getChannelNumber());
            format.writeField("start_date", channelNumbering.getStartDate());
        }

    }

    @Nonnull
    @Override
    public String fieldName(ChannelGroupMembership entity) {
        return "channel";
    }
}
