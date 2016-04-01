package org.atlasapi.content.v2.serialization;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.v2.model.udt.Clip;
import org.atlasapi.content.v2.model.udt.Encoding;
import org.atlasapi.content.v2.model.udt.Restriction;
import org.atlasapi.content.v2.model.udt.SegmentEvent;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;

public class ClipSerialization {

    private final ContentSetter contentSetter = new ContentSetter();
    private final EncodingSerialization encoding = new EncodingSerialization();
    private final ContainerRefSerialization containerRef = new ContainerRefSerialization();
    private final ContainerSummarySerialization containerSummary =
            new ContainerSummarySerialization();
    private final BroadcastSerialization broadcast = new BroadcastSerialization();
    private final SegmentEventSerialization segmentEvent = new SegmentEventSerialization();
    private final RestrictionSerialization restriction = new RestrictionSerialization();

    public Clip serialize(org.atlasapi.content.Clip clip) {
        if (clip == null) {
            return null;
        }
        Clip internal =
                new Clip();

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
                .map(broadcast::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setSegmentEvents(clip.getSegmentEvents()
                .stream()
                .map(segmentEvent::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setRestrictions(clip.getRestrictions()
                .stream()
                .map(restriction::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

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

        Set<org.atlasapi.content.v2.model.udt.Broadcast> broadcasts = internal.getBroadcasts();
        if (broadcasts != null) {
            content.setBroadcasts(broadcasts.stream()
                    .map(broadcast::deserialize)
                    .collect(Collectors.toSet()));
        }

        List<SegmentEvent> segmentEvents = internal.getSegmentEvents();
        if (segmentEvents != null) {
            content.setSegmentEvents(segmentEvents.stream()
                    .map(segmentEvent::deserialize)
                    .collect(Collectors.toList()));
        }

        Set<Restriction> restrictions = internal.getRestrictions();
        if (restrictions != null) {
            content.setRestrictions(restrictions.stream()
                    .map(restriction::deserialize)
                    .collect(Collectors.toSet()));
        }

        content.setClipOf(internal.getClipOf());

        return content;
    }


}