package org.atlasapi.content;

import com.google.common.base.MoreObjects;
import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.IdentifiedSerializer;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Broadcast.Builder;

import com.metabroadcast.common.time.DateTimeZones;

public class BroadcastSerializer {

    private final IdentifiedSerializer<Broadcast> identifiedSerializer;
    private final BlackoutRestrictionSerializer blackoutRestrictionSerializer;
    private final DateTimeSerializer serializer;

    private BroadcastSerializer() {
        this.identifiedSerializer = new IdentifiedSerializer<>();
        this.blackoutRestrictionSerializer = new BlackoutRestrictionSerializer();
        this.serializer = new DateTimeSerializer();
    }

    public static BroadcastSerializer create() {
        return new BroadcastSerializer();
    }

    public ContentProtos.Broadcast.Builder serialize(Broadcast broadcast) {
        Builder builder = ContentProtos.Broadcast.newBuilder();
        builder.setIdentification(identifiedSerializer.serialize(broadcast));
        builder.setChannel(CommonProtos.Reference.newBuilder()
                .setId(broadcast.getChannelId().longValue()));
        builder.setTransmissionTime(serializer.serialize(broadcast.getTransmissionTime()));
        builder.setTransmissionEndTime(serializer.serialize(broadcast.getTransmissionEndTime()));
        if (broadcast.getScheduleDate() != null) {
            builder.setScheduleDate(serializer.serialize(broadcast.getScheduleDate()
                    .toDateTimeAtStartOfDay(DateTimeZones.UTC)));
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
        if (broadcast.getNewEpisode() != null) {
            builder.setNewEpisode(broadcast.getNewEpisode());
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
            builder.setBlackoutRestriction(
                    blackoutRestrictionSerializer.serialize(broadcast.getBlackoutRestriction()
                            .get())
            );
        }
        if (broadcast.getRevisedRepeat() != null) {
            builder.setRevisedRepeat(broadcast.getRevisedRepeat());
        }
        return builder;
    }

    public Broadcast deserialize(ContentProtos.Broadcast msg) {
        Broadcast broadcast = new Broadcast(
                Id.valueOf(msg.getChannel().getId()),
                serializer.deserialize(msg.getTransmissionTime()),
                serializer.deserialize(msg.getTransmissionEndTime())
        );
        identifiedSerializer.deserialize(msg.getIdentification(), broadcast);
        if (msg.hasScheduleDate()) {
            broadcast.setScheduleDate(serializer.deserialize(msg.getScheduleDate())
                    .toLocalDate());
        }
        broadcast.withId(msg.hasSourceId() ? msg.getSourceId() : null);
        broadcast.setIsActivelyPublished(msg.hasActivelyPublished()
                                         ? msg.getActivelyPublished()
                                         : null);
        broadcast.setRepeat(msg.hasRepeat() ? msg.getRepeat() : null);
        broadcast.setSubtitled(msg.hasSubtitled() ? msg.getSubtitled() : null);
        broadcast.setSigned(msg.hasSigned() ? msg.getSigned() : null);
        broadcast.setAudioDescribed(msg.hasAudioDescribed() ? msg.getAudioDescribed() : null);
        broadcast.setHighDefinition(msg.hasHighDefinition() ? msg.getHighDefinition() : null);
        broadcast.setWidescreen(msg.hasWidescreen() ? msg.getWidescreen() : null);
        broadcast.setSurround(msg.hasSurround() ? msg.getSurround() : null);
        broadcast.setLive(msg.hasLive() ? msg.getLive() : null);
        broadcast.setNewSeries(msg.hasNewSeries() ? msg.getNewSeries() : null);
        if (msg.hasNewEpisode()) {
            broadcast.setNewEpisode(msg.getNewEpisode());
        }
        broadcast.setPremiere(msg.hasPremiere() ? msg.getPremiere() : null);
        broadcast.set3d(msg.hasIsThreeD() ? msg.getIsThreeD() : null);
        broadcast.setVersionId(msg.hasVersion() ? msg.getVersion() : null);
        if (msg.hasBlackoutRestriction()) {
            broadcast.setBlackoutRestriction(
                    blackoutRestrictionSerializer.deserialize(msg.getBlackoutRestriction())
            );
        }
        broadcast.setRevisedRepeat(msg.hasRevisedRepeat() ? msg.getRevisedRepeat() : null);
        return broadcast;
    }
}
