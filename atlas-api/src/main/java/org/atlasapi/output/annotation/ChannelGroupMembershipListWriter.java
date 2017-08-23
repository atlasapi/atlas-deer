package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ChannelGroupWriter;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.output.writers.SourceWriter.sourceListWriter;

public class ChannelGroupMembershipListWriter implements EntityListWriter<ChannelGroupMembership> { //TODO: resolve channel groups

    private static final ChannelGroupWriter CHANNEL_GROUP_WRITER = new ChannelGroupWriter(
            "channel_groups",
            "channel_group"
    );
    private static final EntityListWriter<Publisher> PUBLISHER_WRITER = sourceListWriter("publisher");

    private final String listName;
    private final String fieldName;
    private final ChannelGroupResolver channelGroupResolver;

    public ChannelGroupMembershipListWriter(
            String listName,
            String fieldName,
            ChannelGroupResolver channelGroupResolver
    ) {
        this.listName = checkNotNull(listName);
        this.fieldName = checkNotNull(fieldName);
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
    }

    @Override
    public String listName() {
        return listName;
    }

    @Override
    public void write(ChannelGroupMembership entity, FieldWriter format, OutputContext ctxt)
            throws IOException {
        ChannelGroup channelGroup = Futures.get(
                Futures.transform(
                        this.channelGroupResolver.resolveIds(ImmutableSet.of(entity.getChannel()
                                .getId())),
                        (Resolved<ChannelGroup<?>> input) -> {
                            return input.getResources().first().get();
                        }
                ), 1, TimeUnit.MINUTES, IOException.class
        );
        format.writeObject(CHANNEL_GROUP_WRITER, "channel_group", channelGroup, ctxt);
        if (entity instanceof ChannelNumbering) {
            ChannelNumbering channelNumbering = ((ChannelNumbering) entity);
            format.writeField("channel_number", channelNumbering.getChannelNumber().orElse(null));
            format.writeField("start_date", channelNumbering.getStartDate());
        }

        format.writeObject(PUBLISHER_WRITER, entity.getChannel().getSource(), ctxt);

    }

    @Override
    public String fieldName(ChannelGroupMembership entity) {
        return fieldName;
    }
}
