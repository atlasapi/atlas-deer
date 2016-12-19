package org.atlasapi.output.writers;

import java.io.IOException;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.entity.Alias;
import org.atlasapi.output.ChannelGroupSummaryWriter;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public final class BroadcastWriter implements EntityListWriter<Broadcast> {

    private static final Logger log = LoggerFactory.getLogger(BroadcastWriter.class);

    private static final String ELEMENT_NAME = "broadcast";

    private final AliasWriter aliasWriter;
    private final BroadcastIdAliasMapping aliasMapping;
    private final BlackoutRestrictionWriter blackoutRestrictionWriter;

    private final String listName;
    private final NumberToShortStringCodec codec;
    private final ChannelResolver channelResolver;
    private final ChannelWriter channelWriter;

    public BroadcastWriter(
            String listName,
            NumberToShortStringCodec codec,
            ChannelResolver channelResolver
    ) {
        this.aliasWriter = new AliasWriter();
        this.aliasMapping = new BroadcastIdAliasMapping();
        this.blackoutRestrictionWriter = new BlackoutRestrictionWriter();

        this.listName = checkNotNull(listName);
        this.codec = checkNotNull(codec);
        this.channelResolver = checkNotNull(channelResolver);
        this.channelWriter = new ChannelWriter(
                "channels",
                "channel",
                new ChannelGroupSummaryWriter(new SubstitutionTableNumberCodec())
        );
    }

    public static BroadcastWriter create(
            String listName,
            NumberToShortStringCodec codec,
            ChannelResolver channelResolver
    ) {
        return new BroadcastWriter(listName, codec, channelResolver);
    }

    @Override
    public void write(Broadcast entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        ImmutableList.Builder<Alias> aliases = ImmutableList.builder();
        Alias idAlias = aliasMapping.apply(entity);
        if (idAlias != null) {
            aliases.add(idAlias);
        }
        aliases.addAll(entity.getAliases());
        writer.writeList(aliasWriter, aliases.build(), ctxt);
        writer.writeField("transmission_time", entity.getTransmissionTime());
        writer.writeField("transmission_end_time", entity.getTransmissionEndTime());
        writer.writeField(
                "broadcast_duration",
                Ints.saturatedCast(entity.getBroadcastDuration().getStandardSeconds())
        );
        writer.writeField("broadcast_on", codec.encode(entity.getChannelId().toBigInteger()));

        Channel channel = Futures.get(
                channelResolver.resolveIds(
                        ImmutableList.of(entity.getChannelId())
                ),
                IOException.class
        )
                .getResources()
                .first()
                .orNull();

        if (channel == null) {
            log.error("Unable to resolve channel {}", entity.getChannelId());

        } else {
            // Little hack until Broadcasts have their own composite objects to deal with resolution of
            // channels and channel groups outside annotation/writer logic

            ResolvedChannel resolvedChannel = ResolvedChannel.builder(channel).build();
            writer.writeObject(channelWriter, resolvedChannel, ctxt);
        }

        writer.writeField("schedule_date", entity.getScheduleDate());
        writer.writeField("repeat", entity.getRepeat());
        writer.writeField("subtitled", entity.getSubtitled());
        writer.writeField("signed", entity.getSigned());
        writer.writeField("audio_described", entity.getAudioDescribed());
        writer.writeField("high_definition", entity.getHighDefinition());
        writer.writeField("widescreen", entity.getWidescreen());
        writer.writeField("surround", entity.getSurround());
        writer.writeField("live", entity.getLive());
        writer.writeField("premiere", entity.getPremiere());
        writer.writeField("new_series", entity.getNewSeries());
        writer.writeField("new_episode", entity.getNewEpisode());
        writer.writeField("revised_repeat", entity.getRevisedRepeat());

        if (!entity.getBlackoutRestriction().isPresent()) {
            writer.writeField("blackout_restriction", null);
        } else {
            writer.writeObject(
                    blackoutRestrictionWriter,
                    entity.getBlackoutRestriction().get(),
                    ctxt
            );
        }
    }

    @Override
    public String listName() {
        return listName;
    }

    @Override
    public String fieldName(Broadcast entity) {
        return ELEMENT_NAME;
    }
}
