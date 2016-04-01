package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Broadcast;
import org.atlasapi.entity.Id;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;

public class BroadcastSerialization {

    private final IdentifiedSetter identifiedSetter = new IdentifiedSetter();

    public Broadcast serialize(org.atlasapi.content.Broadcast broadcast) {
        if (broadcast == null) {
            return null;
        }
        Broadcast internal =
                new Broadcast();

        identifiedSetter.serialize(internal, broadcast);

        Id channelId = broadcast.getChannelId();
        if (channelId != null) {
            internal.setChannelId(channelId.longValue());
        }

        DateTime transmissionTime = broadcast.getTransmissionTime();
        if (transmissionTime != null) {
            internal.setTransmissionStart(transmissionTime.toInstant());
        }

        DateTime transmissionEndTime = broadcast.getTransmissionEndTime();
        if (transmissionEndTime != null) {
            internal.setTransmissionEnd(transmissionEndTime.toInstant());
        }

        Duration broadcastDuration = broadcast.getBroadcastDuration();
        if (broadcastDuration != null) {
            internal.setBroadcastDuration(broadcastDuration.getMillis());
        }
        internal.setScheduleDate(broadcast.getScheduleDate());
        internal.setActivelyPublished(broadcast.isActivelyPublished());
        internal.setSourceId(broadcast.getSourceId());
        internal.setVersionId(broadcast.getVersionId());
        internal.setRepeat(broadcast.getRepeat());
        internal.setSubtitled(broadcast.getSubtitled());
        internal.setSigned(broadcast.getSigned());
        internal.setAudioDescribed(broadcast.getAudioDescribed());
        internal.setHighDefinition(broadcast.getHighDefinition());
        internal.setWidescreen(broadcast.getWidescreen());
        internal.setSurround(broadcast.getSurround());
        internal.setLive(broadcast.getLive());
        internal.setNewSeries(broadcast.getNewSeries());
        internal.setNewEpisode(broadcast.getNewEpisode());
        internal.setPremiere(broadcast.getPremiere());
        internal.setIs3d(broadcast.is3d());
        internal.setBlackoutRestriction(broadcast.getBlackoutRestriction().isPresent()
                && broadcast.getBlackoutRestriction().get().getAll());

        return internal;
    }

    public org.atlasapi.content.Broadcast deserialize(Broadcast internal) {
        if (internal == null) {
            return null;
        }

        org.atlasapi.content.Broadcast broadcast = new org.atlasapi.content.Broadcast(
                Id.valueOf(internal.getChannelId()),
                toDateTime(internal.getTransmissionStart()),
                toDateTime(internal.getTransmissionEnd()),
                internal.getActivelyPublished()
        );

        identifiedSetter.deserialize(broadcast, internal);

        broadcast.setScheduleDate(internal.getScheduleDate());
        broadcast.setIsActivelyPublished(internal.getActivelyPublished());
        broadcast = broadcast.withId(internal.getSourceId());
        broadcast.setVersionId(internal.getVersionId());
        broadcast.setRepeat(internal.getRepeat());
        broadcast.setSubtitled(internal.getSubtitled());
        broadcast.setSigned(internal.getSigned());
        broadcast.setAudioDescribed(internal.getAudioDescribed());
        broadcast.setHighDefinition(internal.getHighDefinition());
        broadcast.setWidescreen(internal.getWidescreen());
        broadcast.setSurround(internal.getSurround());
        broadcast.setLive(internal.getLive());
        broadcast.setNewSeries(internal.getNewSeries());
        broadcast.setNewEpisode(internal.getNewEpisode());
        broadcast.setPremiere(internal.getPremiere());
        broadcast.set3d(internal.getIs3d());
        broadcast.setBlackoutRestriction(
                new org.atlasapi.content.BlackoutRestriction(internal.getBlackoutRestriction()));

        return broadcast;
    }

}