/* Copyright 2009 British Broadcasting Corporation
 Copyright 2009 Meta Broadcast Ltd

 Licensed under the Apache License, Version 2.0 (the "License"); you
 may not use this file except in compliance with the License. You may
 obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License. */
package org.atlasapi.content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.intl.Country;
import org.atlasapi.entity.Id;
import org.atlasapi.hashing.ExcludeFromHash;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;
import org.atlasapi.segment.SegmentEvent;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class Item extends Content {

    private ContainerRef containerRef;
    private boolean isLongForm = false;
    private Boolean blackAndWhite;
    private Set<Country> countriesOfOrigin = Sets.newHashSet();

    @ExcludeFromHash
    private String sortKey;

    private ContainerSummary containerSummary;
    private Set<Broadcast> broadcasts = Sets.newLinkedHashSet();
    private List<SegmentEvent> segmentEvents = ImmutableList.of();
    private Set<Restriction> restrictions = Sets.newHashSet();

    public Item(String uri, String curie, Publisher publisher) {
        super(uri, curie, publisher);
    }

    public Item(Id id, Publisher source) {
        super(id, source);
    }

    public Item() {
    }

    @Override
    public ItemRef toRef() {
        return new ItemRef(
                getId(),
                getSource(),
                SortKey.keyFrom(this),
                getThisOrChildLastUpdated()
        );
    }

    public void setContainerRef(@Nullable ContainerRef containerRef) {
        this.containerRef = containerRef;
    }

    public void setContainer(Container container) {
        setContainerRef(container.toRef());
    }

    @Nullable
    @FieldName("container_ref")
    public ContainerRef getContainerRef() {
        return containerRef;
    }

    @FieldName("is_long_form")
    public boolean getIsLongForm() {
        return isLongForm;
    }

    public void setIsLongForm(boolean isLongForm) {
        this.isLongForm = isLongForm;
    }

    @FieldName("countries_of_origin")
    public Set<Country> getCountriesOfOrigin() {
        return countriesOfOrigin;
    }

    public void setCountriesOfOrigin(Iterable<Country> countries) {
        this.countriesOfOrigin = Sets.newHashSet(countries);
    }

    @FieldName("people")
    public List<CrewMember> getPeople() {
        return people();
    }

    public void setBlackAndWhite(Boolean blackAndWhite) {
        this.blackAndWhite = blackAndWhite;
    }

    @FieldName("black_and_white")
    public Boolean getBlackAndWhite() {
        return blackAndWhite;
    }

    @Nullable
    @FieldName("broadcasts")
    public Set<Broadcast> getBroadcasts() {
        return broadcasts;
    }

    public void setBroadcasts(@Nullable Set<Broadcast> broadcasts) {
        this.broadcasts = broadcasts;
    }

    public void addBroadcast(Broadcast broadcast) {
        checkNotNull(broadcast);
        broadcasts.add(broadcast);
    }

    public void setRestrictions(Set<Restriction> restrictions) {
        this.restrictions = restrictions == null ? Sets.newHashSet() : restrictions;
    }

    public void addRestriction(Restriction restriction) {
        checkNotNull(restriction);
        restrictions.add(restriction);
    }

    @FieldName("restrictions")
    public Set<Restriction> getRestrictions() {
        return restrictions;
    }

    @FieldName("segment_events")
    public List<SegmentEvent> getSegmentEvents() {
        return segmentEvents;
    }

    public void setSegmentEvents(Iterable<SegmentEvent> segmentEvents) {
        this.segmentEvents = SegmentEvent.ORDERING.immutableSortedCopy(segmentEvents);
    }

    public void addSegmentEvents(Iterable<SegmentEvent> segmentEvents) {
        this.segmentEvents = SegmentEvent.ORDERING.immutableSortedCopy(ImmutableSet.<SegmentEvent>builder()
                .addAll(segmentEvents)
                .addAll(this.segmentEvents).build());
    }

    public static Item copyTo(Item from, Item to) {
        Content.copyTo(from, to);
        to.containerRef = from.containerRef;
        to.containerSummary = from.containerSummary;
        to.isLongForm = from.isLongForm;
        to.broadcasts = from.broadcasts == null ? null : Sets.newLinkedHashSet(from.broadcasts);
        to.segmentEvents = SegmentEvent.ORDERING.immutableSortedCopy(from.segmentEvents);
        to.restrictions = Sets.newHashSet(from.restrictions);
        to.blackAndWhite = from.blackAndWhite;
        to.countriesOfOrigin = Sets.newHashSet(from.countriesOfOrigin);
        return to;
    }

    @Override public <T extends Described> T copyTo(T to) {
        if (to instanceof Item) {
            copyTo(this, (Item) to);
            return to;
        }
        return super.copyTo(to);
    }

    @Override public Item copy() {
        return copyTo(this, new Item());
    }

    public Item withSortKey(String sortKey) {
        this.sortKey = sortKey;
        return this;
    }

    @FieldName("sort_key")
    public String sortKey() {
        return sortKey;
    }

    public boolean isChild() {
        return this.containerRef != null;
    }

    @Nullable
    @FieldName("container_summary")
    public ContainerSummary getContainerSummary() {
        return containerSummary;
    }

    public void setContainerSummary(ContainerSummary containerSummary) {
        this.containerSummary = containerSummary;
    }

    public <V> V accept(ItemVisitor<V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public <V> V accept(ContentVisitor<V> visitor) {
        return accept((ItemVisitor<V>) visitor);
    }

    @Override
    protected String getSortKey() {
        return SortKey.keyFrom(this);
    }

    public ItemSummary toSummary() {
        return new ItemSummary(
                toRef(),
                getTitle(),
                getDescription(),
                getImage(),
                getYear(),
                getCertificates()
        );
    }

    public Iterable<BroadcastRef> getUpcomingBroadcastRefs() {
        return broadcasts
                .stream()
                .filter(Broadcast::isUpcoming)
                .map(Broadcast::toRef)
                .collect(Collectors.toSet());

    }
}
