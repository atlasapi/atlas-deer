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

public class ChannelGroupMembershipListWriter implements EntityListWriter<ChannelGroupMembership> {

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
        ChannelGroup channelGroup = Futures.getChecked(
                Futures.transform(
                        this.channelGroupResolver.resolveIds(
                                ImmutableSet.of(entity.getChannelGroup().getId())
                        ),
                        (Resolved<ChannelGroup<?>> input) -> input.getResources().first().get()
                ), IOException.class, 1, TimeUnit.MINUTES
        );

        // need to resolve before filtering,
        // as the entity.getChannelGroup().getSource() is always metabroadcast.com
        if (ctxt.getApplication().getConfiguration().isReadEnabled(channelGroup.getSource())) {
            format.writeObject(CHANNEL_GROUP_WRITER, "channel_group", channelGroup, ctxt);
            if (entity instanceof ChannelNumbering) {
                ChannelNumbering channelNumbering = ((ChannelNumbering) entity);
                format.writeField("channel_number",
                        channelNumbering.getChannelNumber().orElse(null));
                format.writeField("start_date", channelNumbering.getStartDate());
            }

            format.writeObject(PUBLISHER_WRITER, entity.getChannelGroup().getSource(), ctxt);
        }
    }

    @Override
    public String fieldName(ChannelGroupMembership entity) {
        return fieldName;
    }
}
