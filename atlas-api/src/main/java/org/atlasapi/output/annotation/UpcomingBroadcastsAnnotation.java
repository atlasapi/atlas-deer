package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelEquivRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ChannelsBroadcastFilter;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Marker for {@link BroadcastsAnnotation} to only write broadcasts that have not yet broadcast.
 */
public class UpcomingBroadcastsAnnotation extends OutputAnnotation<Content, ResolvedContent> { //TODO: add resolution

    private static final Logger log = LoggerFactory.getLogger(UpcomingBroadcastsAnnotation.class);

    private final BroadcastWriter broadcastWriter;
    private final ChannelsBroadcastFilter channelsBroadcastFilter;
    private final ChannelResolver channelResolver;

    private UpcomingBroadcastsAnnotation(
            BroadcastWriter broadcastWriter,
            ChannelResolver channelResolver
    ) {
        this.broadcastWriter = broadcastWriter;
        this.channelsBroadcastFilter = ChannelsBroadcastFilter.create();
        this.channelResolver = channelResolver;
    }

    public static UpcomingBroadcastsAnnotation create(
            NumberToShortStringCodec codec,
            ChannelResolver channelResolver
    ) {
        return new UpcomingBroadcastsAnnotation(
                BroadcastWriter.create(
                        "broadcasts",
                        "broadcast",
                        codec
                ),
                channelResolver
        );
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity.getContent() instanceof Item) {
            Item item = (Item) entity.getContent();
            Stream<Broadcast> broadcastStream = item.getBroadcasts().stream()
                    .filter(Broadcast::isActivelyPublished)
                    .filter(b -> b.getTransmissionTime().isAfter(DateTime.now(DateTimeZone.UTC)));

            if (ctxt.getRegion().isPresent()) {
                List<ResolvedBroadcast> resolvedBroadcasts = StreamSupport.stream(
                        channelsBroadcastFilter.sortAndFilter(
                                broadcastStream.collect(MoreCollectors.toImmutableList()),
                                ctxt.getRegion().get()
                        ).spliterator(),
                        false
                )
                        .map(broadcast -> ResolvedBroadcast.create(broadcast, resolveChannel(broadcast)))
                        .collect(MoreCollectors.toImmutableList());

                writer.writeList(
                        broadcastWriter,
                        resolvedBroadcasts,
                        ctxt
                );
            } else {
                writer.writeList(
                        broadcastWriter,
                        broadcastStream.map(broadcast -> ResolvedBroadcast.create(
                                broadcast,
                                resolveChannel(broadcast))
                        )
                                .collect(MoreCollectors.toImmutableList()),
                        ctxt
                );
            }
        }
    }

    //TODO: Move resolution logic to query executor
    private ResolvedChannel resolveChannel(Broadcast broadcast) {

        try {
            Channel channel = Futures.getChecked(
                    channelResolver.resolveIds(
                            ImmutableList.of(broadcast.getChannelId())
                    ),
                    IOException.class
            )
                    .getResources()
                    .first()
                    .orNull();

            return ResolvedChannel.builder()
                    .withChannel(channel)
                    .withResolvedEquivalents(resolveEquivalents(channel.getSameAs()))
                    .build();

        } catch (IOException e) {
            log.error("Failed to resolveContent channel: {}", broadcast.getChannelId(), e);
            return null;
        }

    }

    @Nullable
    private Iterable<Channel> resolveEquivalents(Set<ChannelEquivRef> channelRefs) {
        try {
            if (channelRefs != null && !channelRefs.isEmpty()) {
                Iterable<Id> ids = Iterables.transform(channelRefs, ResourceRef::getId);
                return channelResolver.resolveIds(ids).get(1, TimeUnit.MINUTES).getResources();
            }

            return null;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to resolveContent channel equivlents", e);
            return null;
        }
    }
}
