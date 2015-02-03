package org.atlasapi.output.annotation;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelWriter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelVariationAnnotation extends OutputAnnotation<Channel> {

    private final ChannelResolver channelResolver;
    private final ChannelWriter CHANNEL_WRITER = new ChannelWriter("variations", "variation");

    public ChannelVariationAnnotation(
            ChannelResolver channelResolver
    ) {
        this.channelResolver = checkNotNull(channelResolver);
    }

    @Override
    public void write(Channel entity, FieldWriter format, OutputContext ctxt) throws IOException {
        Iterable<Id> ids = Iterables.transform(entity.getVariations(), new Function<ChannelRef, Id>() {
            @Override
            public Id apply(ChannelRef input) {
                return input.getId();
            }
        });

        Iterable<Channel> channels = Futures.get(Futures.transform(channelResolver.resolveIds(ids), new Function<Resolved<Channel>, Iterable<Channel>>() {
            @Override
            public Iterable<Channel> apply(@Nullable Resolved<Channel> input) {
                return input.getResources();
            }
        }), 1, TimeUnit.MINUTES, IOException.class);
        format.writeList(CHANNEL_WRITER, channels, ctxt);
    }
}
