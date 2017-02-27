package org.atlasapi.output.writers;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.channel.Channel;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.content.Broadcast;
import org.atlasapi.entity.Alias;
import org.atlasapi.output.ChannelGroupSummaryWriter;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelWriter;

import java.io.IOException;

public class AggregatedBroadcastWriter implements EntityListWriter<ResolvedBroadcast> {

    private static final String listName = "aggregated_broadcasts";
    private static final String fieldName = "aggregated_broadcast";

    private final NumberToShortStringCodec codec;
    private final AliasWriter aliasWriter;
    private final BroadcastIdAliasMapping aliasMapping;
    private final BlackoutRestrictionWriter blackoutRestrictionWriter;
    private final ChannelWriter channelWriter;

    private AggregatedBroadcastWriter(NumberToShortStringCodec codec) {
        this.codec = codec;

        this.aliasWriter = new AliasWriter();
        this.aliasMapping = new BroadcastIdAliasMapping();
        this.blackoutRestrictionWriter = new BlackoutRestrictionWriter();
        this.channelWriter = ChannelWriter.create(
                "channels",
                "channel",
                ChannelGroupSummaryWriter.create(new SubstitutionTableNumberCodec())
        );
    }

    public static AggregatedBroadcastWriter create(NumberToShortStringCodec codec) {
        return new AggregatedBroadcastWriter(codec);
    }

    @Override
    public void write(
            ResolvedBroadcast entity,
            FieldWriter writer,
            OutputContext ctxt
    ) throws IOException {

        Broadcast broadcast = entity.getBroadcast();
        Channel channel = entity.getResolvedChannel().getChannel();

        ImmutableList.Builder<Alias> aliases = ImmutableList.builder();
        Alias idAlias = aliasMapping.apply(broadcast);
        if (idAlias != null) {
            aliases.add(idAlias);
        }
        aliases.addAll(broadcast.getAliases());
        writer.writeList(aliasWriter, aliases.build(), ctxt);
        writer.writeField("transmission_time", broadcast.getTransmissionTime());
        writer.writeField("transmission_end_time", broadcast.getTransmissionEndTime());
        writer.writeField(
                "broadcast_duration",
                Ints.saturatedCast(broadcast.getBroadcastDuration().getStandardSeconds())
        );
        writer.writeField("broadcast_on", codec.encode(channel.getId().toBigInteger()));

        writer.writeObject(channelWriter, entity.getResolvedChannel(), ctxt);

        writer.writeField("schedule_date", broadcast.getScheduleDate());
        writer.writeField("repeat", broadcast.getRepeat());
        writer.writeField("subtitled", broadcast.getSubtitled());
        writer.writeField("signed", broadcast.getSigned());
        writer.writeField("audio_described", broadcast.getAudioDescribed());
        writer.writeField("high_definition", broadcast.getHighDefinition());
        writer.writeField("widescreen", broadcast.getWidescreen());
        writer.writeField("surround", broadcast.getSurround());
        writer.writeField("live", broadcast.getLive());
        writer.writeField("premiere", broadcast.getPremiere());
        writer.writeField("new_series", broadcast.getNewSeries());
        writer.writeField("new_episode", broadcast.getNewEpisode());
        writer.writeField("revised_repeat", broadcast.getRevisedRepeat());

        if (!broadcast.getBlackoutRestriction().isPresent()) {
            writer.writeField("blackout_restriction", null);
        } else {
            writer.writeObject(
                    blackoutRestrictionWriter,
                    broadcast.getBlackoutRestriction().get(),
                    ctxt
            );
        }
    }

    @Override
    public String listName() {
        return listName;
    }

    @Override
    public String fieldName(ResolvedBroadcast resolvedBroadcast) {
        return fieldName;
    }

}
