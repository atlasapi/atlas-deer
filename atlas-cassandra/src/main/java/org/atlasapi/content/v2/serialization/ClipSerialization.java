package org.atlasapi.content.v2.serialization;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.v2.model.Clip;
import org.atlasapi.content.v2.model.Encoding;
import org.atlasapi.content.v2.model.udt.Broadcast;
import org.atlasapi.content.v2.model.udt.SegmentEvent;
import org.atlasapi.content.v2.model.udt.UpdateTimes;
import org.atlasapi.content.v2.serialization.setters.ContentSetter;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;
import com.metabroadcast.common.stream.MoreCollectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.content.v2.serialization.DateTimeUtils.toInstant;

public class ClipSerialization {

    private final ContentSetter contentSetter;
    private final EncodingSerialization encoding = new EncodingSerialization();
    private final ContainerRefSerialization containerRef = new ContainerRefSerialization();
    private final ContainerSummarySerialization containerSummary =
            new ContainerSummarySerialization();
    private final BroadcastSerialization broadcast = new BroadcastSerialization();
    private final SegmentEventSerialization segmentEvent = new SegmentEventSerialization();
    private final RestrictionSerialization restriction = new RestrictionSerialization();

    public ClipSerialization(ContentSetter parent) {
        this.contentSetter = checkNotNull(parent);
    }

    public Clip serialize(org.atlasapi.content.Clip clip) {
        if (clip == null) {
            return null;
        }
        Clip internal = new Clip();

        contentSetter.serialize(internal, clip);

        internal.setManifestedAs(clip.getManifestedAs().stream()
                .map(encoding::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setContainerRef(containerRef.serialize(clip.getContainerRef()));

        internal.setIsLongForm(clip.getIsLongForm());
        internal.setBlackAndWhite(clip.getBlackAndWhite());
        internal.setCountriesOfOrigin(
                clip.getCountriesOfOrigin().stream()
                        .map(Country::code)
                        .collect(Collectors.toSet())
        );
        internal.setSortKey(clip.sortKey());

        internal.setContainerSummary(containerSummary.serialize(clip.getContainerSummary()));

        internal.setBroadcasts(clip.getBroadcasts().stream()
                .collect(MoreCollectors.toImmutableMap(
                        org.atlasapi.content.Broadcast::getSourceId,
                        broadcast::serialize
                )));

        internal.setSegmentEvents(clip.getSegmentEvents()
                .stream()
                .map(segmentEvent::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setRestrictions(
                clip.getRestrictions().stream()
                        .map(r -> new Clip.RestrictionWithTimes(
                                restriction.serialize(r),
                                new UpdateTimes(
                                        toInstant(r.getLastUpdated()),
                                        toInstant(r.getEquivalenceUpdate())
                                )
                        )).collect(MoreCollectors.toImmutableList())

        );
        internal.setClipOf(clip.getClipOf());

        return internal;
    }

    public org.atlasapi.content.Clip deserialize(Clip internal) {
        org.atlasapi.content.Clip content = new org.atlasapi.content.Clip();

        contentSetter.deserialize(content, internal);

        Set<Encoding> manifestedAs = internal.getManifestedAs();
        if (manifestedAs != null) {
            content.setManifestedAs(manifestedAs.stream()
                    .map(encoding::deserialize)
                    .collect(Collectors.toSet()));
        }

        if (internal.getIsLongForm() != null) {
            content.setIsLongForm(internal.getIsLongForm());
        }
        content.setBlackAndWhite(internal.getBlackAndWhite());
        Set<String> countriesOfOrigin = internal.getCountriesOfOrigin();
        if (countriesOfOrigin != null) {
            content.setCountriesOfOrigin(countriesOfOrigin.stream()
                    .map(Countries::fromCode)
                    .collect(Collectors.toSet()));
        }

        content = (org.atlasapi.content.Clip) content.withSortKey(internal.getSortKey());

        content.setContainerSummary(containerSummary.deserialize(internal.getContainerSummary()));

        content.setContainerRef(containerRef.deserialize(internal.getContainerRef()));

        Map<String, Broadcast> broadcasts = internal.getBroadcasts();
        if (broadcasts != null) {
            content.setBroadcasts(broadcasts.entrySet().stream()
                    .map(entry -> broadcast.deserialize(entry.getKey(), entry.getValue()))
                    .collect(MoreCollectors.toImmutableSet()));
        }

        List<SegmentEvent> segmentEvents = internal.getSegmentEvents();
        if (segmentEvents != null) {
            content.setSegmentEvents(segmentEvents.stream()
                    .map(segmentEvent::deserialize)
                    .collect(Collectors.toList()));
        }

        List<Clip.RestrictionWithTimes> restrictions = internal.getRestrictions();
        if (restrictions != null) {
            content.setRestrictions(
                    restrictions.stream()
                            .map(r -> restriction.deserialize(
                                    r.getUpdateTimes(),
                                    r.getRestriction()
                            )).collect(MoreCollectors.toImmutableSet())
            );
        }

        content.setClipOf(internal.getClipOf());

        return content;
    }


}