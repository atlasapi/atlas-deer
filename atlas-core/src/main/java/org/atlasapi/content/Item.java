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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;
import org.atlasapi.segment.SegmentEvent;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.intl.Country;

/**
 * @author Robert Chatley (robert@metabroadcast.com)
 * @author Lee Denison (lee@metabroadcast.com)
 * @author John Ayres (john@metabroadcast.com)
 */
public class Item extends Content {

    private ContainerRef containerRef;
    private boolean isLongForm = false;
    private Boolean blackAndWhite;
    private Set<Country> countriesOfOrigin = Sets.newHashSet();
    private String sortKey;
    private ContainerSummary containerSummary;
    private Set<Encoding> manifestedAs = Sets.newLinkedHashSet();
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
        return new ItemRef(getId(), getSource(), SortKey.keyFrom(this), getThisOrChildLastUpdated());
    }

    public void setContainerRef(ContainerRef containerRef) {
        this.containerRef = containerRef;
    }

    public void setContainer(Container container) {
        setContainerRef(container.toRef());
    }

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

    public void setCountriesOfOrigin(Set<Country> countries) {
        this.countriesOfOrigin = Sets.newHashSet();
        for (Country country : countries) {
            countriesOfOrigin.add(country);
        }
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
    
    @FieldName("manifested_as")
    public Set<Encoding> getManifestedAs() {
        return manifestedAs;
    }

    public void setManifestedAs(Set<Encoding> manifestedAs) {
        this.manifestedAs = manifestedAs;
    }
    
    public void addManifestedAs(Encoding encoding) {
        checkNotNull(encoding);
        manifestedAs.add(encoding);
    }
    
    @FieldName("broadcasts")
    public Set<Broadcast> getBroadcasts() {
        return broadcasts;
    }

    public void setBroadcasts(Set<Broadcast> broadcasts) {
        this.broadcasts = broadcasts;
    }
    
    public void addBroadcast(Broadcast broadcast) {
        checkNotNull(broadcast);
        broadcasts.add(broadcast);
    }

    public void setRestrictions(Set<Restriction> restrictions) {
        this.restrictions = restrictions;
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

    @Override
    public Item copy() {
        return Item.copyTo(this, new Item());
    }

    public static Item copyTo(Item from, Item to) {
        Content.copyTo(from, to);
        if (from.containerRef != null) {
            to.containerRef = from.containerRef;
        }
        to.containerSummary = from.containerSummary;
        to.isLongForm = from.isLongForm;
        to.broadcasts = Sets.newHashSet(from.broadcasts);
        to.manifestedAs = Sets.newHashSet(from.manifestedAs);
        to.segmentEvents = Lists.newArrayList(from.segmentEvents);
        to.restrictions = Sets.newHashSet(from.restrictions);
        to.blackAndWhite = from.blackAndWhite;
        to.countriesOfOrigin = Sets.newHashSet(from.countriesOfOrigin);
        return to;
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
        return this.containerRef == null;
    }

    public static final Function<Item, Item> COPY = new Function<Item, Item>() {

        @Override
        public Item apply(Item input) {
            return (Item) input.copy();
        }
    };

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
        return accept((ItemVisitor<V>)visitor);
    }

    @Override
    protected String getSortKey() {
        return SortKey.keyFrom(this);
    }

    public static final Function<Item, Set<Broadcast>> TO_BROADCASTS = new Function<Item, Set<Broadcast>>() {
        @Override
        public Set<Broadcast> apply(Item input) {
            return input.broadcasts;
        }
    };
    
    public static final Function<Item, Set<Encoding>> TO_ENCODINGS = new Function<Item, Set<Encoding>>() {
        @Override
        public Set<Encoding> apply(Item input) {
            return input.manifestedAs;
        }
    };
    
    public static final Function<Item, List<SegmentEvent>> TO_SEGMENT_EVENTS = new Function<Item, List<SegmentEvent>>() {

        @Override
        public List<SegmentEvent> apply(Item input) {
            return input.segmentEvents;
        }
        
    };
    
}
