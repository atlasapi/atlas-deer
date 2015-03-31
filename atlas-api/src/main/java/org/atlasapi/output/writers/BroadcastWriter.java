package org.atlasapi.output.writers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.content.Broadcast;
import org.atlasapi.entity.Alias;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

public final class BroadcastWriter implements EntityListWriter<Broadcast> {
    
    private static final String ELEMENT_NAME = "broadcast";
    
    private final AliasWriter aliasWriter = new AliasWriter();
    private final BroadcastIdAliasMapping aliasMapping = new BroadcastIdAliasMapping();
    private final BlackoutRestrictionWriter blackoutRestrictionWriter = new BlackoutRestrictionWriter();

    private final String listName;
    private NumberToShortStringCodec codec;

    public BroadcastWriter(String listName, NumberToShortStringCodec codec) {
        this.listName = checkNotNull(listName);
        this.codec = checkNotNull(codec);
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
        writer.writeField("broadcast_duration", Ints.saturatedCast(entity.getBroadcastDuration().getStandardSeconds()));
        writer.writeField("broadcast_on", codec.encode(entity.getChannelId().toBigInteger()));
        writer.writeField("schedule_date", entity.getScheduleDate());
        writer.writeField("repeat", entity.getRepeat());
        writer.writeField("subtitled", entity.getSubtitled());
        writer.writeField("signed",entity.getSigned());
        writer.writeField("audio_described",entity.getAudioDescribed());
        writer.writeField("high_definition",entity.getHighDefinition());
        writer.writeField("widescreen",entity.getWidescreen());
        writer.writeField("surround",entity.getSurround());
        writer.writeField("live",entity.getLive());
        writer.writeField("premiere",entity.getPremiere());
        writer.writeField("new_series",entity.getNewSeries());
        if(!entity.getBlackoutRestriction().isPresent()) {
            writer.writeField("blackout_restriction", null);
        } else {
            writer.writeObject(blackoutRestrictionWriter, entity.getBlackoutRestriction().orNull(), ctxt);
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