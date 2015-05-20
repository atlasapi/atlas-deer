package org.atlasapi.output.annotation;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelWriter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;


public class ParentChannelAnnotation extends OutputAnnotation<Channel> {

    private static final ChannelWriter CHANNEL_WRITER = new ChannelWriter("parents", "parent");

    private final ChannelResolver channelResolver;

    public ParentChannelAnnotation(ChannelResolver channelResolver) {
        this.channelResolver = checkNotNull(channelResolver);
    }

    @Override
    public void write(Channel entity, FieldWriter format, OutputContext ctxt) throws IOException {
        if (entity.getParent() != null) {
            Channel parentChannel = Futures.get(
                    Futures.transform(
                            channelResolver.resolveIds(ImmutableList.of(entity.getParent().getId())),
                            new Function<Resolved<Channel>, Channel>() {
                                @Override
                                public Channel apply(Resolved<Channel> input) {
                                    return input.getResources().first().get();
                                }
                            }
                    ), 1, TimeUnit.MINUTES, IOException.class
            );
            format.writeObject(CHANNEL_WRITER, "parent", parentChannel, ctxt);
        } else {
            format.writeField("parent", null);
        }

    }
}
