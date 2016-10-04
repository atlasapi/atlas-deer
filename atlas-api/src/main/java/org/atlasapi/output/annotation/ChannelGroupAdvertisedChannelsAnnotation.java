package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.ChannelWithChannelGroupMembership;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.MissingResolvedDataException;
import org.atlasapi.query.v4.channelgroup.ChannelGroupChannelWriter;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupAdvertisedChannelsAnnotation extends OutputAnnotation<ResolvedChannelGroup> {

    private final ChannelGroupChannelWriter channelWriter;

    public ChannelGroupAdvertisedChannelsAnnotation(ChannelGroupChannelWriter channelWriter) {
        this.channelWriter = checkNotNull(channelWriter);
    }

    @Override
    public void write(ResolvedChannelGroup entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {

        Optional<ImmutableSet<ChannelWithChannelGroupMembership>> channels =
                entity.getAdvertisedChannels();
        if (channels.isPresent()) {
            writer.writeList(channelWriter, channels.get(), ctxt);
        } else {
            throw new MissingResolvedDataException(channelWriter.listName());
        }
    }

}
