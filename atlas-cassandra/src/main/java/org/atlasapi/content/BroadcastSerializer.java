package org.atlasapi.content;

import static org.atlasapi.entity.ProtoBufUtils.deserializeDateTime;
import static org.atlasapi.entity.ProtoBufUtils.serializeDateTime;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.IdentifiedSerializer;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Broadcast.Builder;

import com.metabroadcast.common.time.DateTimeZones;

public class BroadcastSerializer {

    private final IdentifiedSerializer identifiedSerializer = new IdentifiedSerializer();
    private final BlackoutRestrictionSerializer blackoutRestrictionSerializer = new BlackoutRestrictionSerializer();
    
    public ContentProtos.Broadcast.Builder serialize(Broadcast broadcast) {
        Builder builder = ContentProtos.Broadcast.newBuilder();
        builder.setIdentification(identifiedSerializer.serialize(broadcast));
        builder.setChannel(CommonProtos.Reference.newBuilder().setId(broadcast.getChannelId().longValue()));
        builder.setTransmissionTime(serializeDateTime(broadcast.getTransmissionTime()));
        builder.setTransmissionEndTime(serializeDateTime(broadcast.getTransmissionEndTime()));
        if (broadcast.getScheduleDate() != null) {
            builder.setScheduleDate(serializeDateTime(broadcast.getScheduleDate().toDateTimeAtStartOfDay(DateTimeZones.UTC)));
        }
        if (broadcast.getSourceId() != null) {
            builder.setSourceId(broadcast.getSourceId());
        }
        if (broadcast.isActivelyPublished() != null) {
            builder.setActivelyPublished(broadcast.isActivelyPublished());
        }
        if (broadcast.getRepeat() != null) {
            builder.setRepeat(broadcast.getRepeat());
        }
        if (broadcast.getSubtitled() != null) {
            builder.setSubtitled(broadcast.getSubtitled());
        }
        if (broadcast.getSigned() != null) {
            builder.setSigned(broadcast.getSigned());
        }
        if (broadcast.getAudioDescribed() != null) {
            builder.setAudioDescribed(broadcast.getAudioDescribed());
        }
        if (broadcast.getHighDefinition() != null) {
            builder.setHighDefinition(broadcast.getHighDefinition());
        }
        if (broadcast.getWidescreen() != null) {
            builder.setWidescreen(broadcast.getWidescreen());
        }
        if (broadcast.getSurround() != null) {
            builder.setSurround(broadcast.getSurround());
        }
        if (broadcast.getLive() != null) {
            builder.setLive(broadcast.getLive());
        }
        if (broadcast.getNewSeries() != null) {
            builder.setNewSeries(broadcast.getNewSeries());
        }
        if (broadcast.getPremiere() != null) {
            builder.setPremiere(broadcast.getPremiere());
        }
        if (broadcast.is3d() != null) {
            builder.setIsThreeD(broadcast.is3d());
        }
        if (broadcast.getVersionId() != null) {
            builder.setVersion(broadcast.getVersionId());
        }
        if (broadcast.getBlackoutRestriction().isPresent()) {
            builder.setBlackoutRestriction(blackoutRestrictionSerializer.serialize(broadcast.getBlackoutRestriction().get()));
        }
        return builder;
    }

    public Broadcast deserialize(ContentProtos.Broadcast msg) {
        Broadcast broadcast = new Broadcast(Id.valueOf(msg.getChannel().getId()),
            deserializeDateTime(msg.getTransmissionTime()),
            deserializeDateTime(msg.getTransmissionEndTime()));
        identifiedSerializer.deserialize(msg.getIdentification(), broadcast);
        if (msg.hasScheduleDate()) {
            broadcast.setScheduleDate(deserializeDateTime(msg.getScheduleDate()).toLocalDate());
        }
        broadcast.withId(msg.hasSourceId() ? msg.getSourceId() : null);
        broadcast.setIsActivelyPublished(msg.hasActivelyPublished() ? msg.getActivelyPublished() : null);
        broadcast.setRepeat(msg.hasRepeat() ? msg.getRepeat() : null);
        broadcast.setSubtitled(msg.hasSubtitled() ? msg.getSubtitled() : null);
        broadcast.setSigned(msg.hasSigned() ? msg.getSigned() : null);
        broadcast.setAudioDescribed(msg.hasAudioDescribed() ? msg.getAudioDescribed() : null);
        broadcast.setHighDefinition(msg.hasHighDefinition() ? msg.getHighDefinition() : null);
        broadcast.setWidescreen(msg.hasWidescreen() ? msg.getWidescreen() : null);
        broadcast.setSurround(msg.hasSurround() ? msg.getSurround() : null);
        broadcast.setLive(msg.hasLive() ? msg.getLive() : null);
        broadcast.setNewSeries(msg.hasNewSeries() ? msg.getNewSeries() : null);
        broadcast.setPremiere(msg.hasPremiere() ? msg.getPremiere() : null);
        broadcast.set3d(msg.hasIsThreeD() ? msg.getIsThreeD() : null);
        broadcast.setVersionId(msg.hasVersion() ? msg.getVersion() : null);
        if (msg.hasBlackoutRestriction()) {
            broadcast.setBlackoutRestriction(blackoutRestrictionSerializer.deserialize(msg.getBlackoutRestriction()));
        }
        return broadcast;
    }
}
