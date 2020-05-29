package org.atlasapi.content;

import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public abstract class Container extends Content {

    protected ImmutableList<ItemRef> itemRefs = ImmutableList.of();
    private Map<ItemRef, Iterable<BroadcastRef>> upcomingContent = ImmutableMap.of();
    private Map<ItemRef, Iterable<LocationSummary>> availableContent = ImmutableMap.of();
    private List<ItemSummary> itemSummaries = ImmutableList.of();

    public Container(String uri, String curie, Publisher publisher) {
        super(uri, curie, publisher);
    }

    public Container(Id id, Publisher source) {
        super(id, source);
    }

    public Container() {
    }

    @Nullable
    @FieldName("item_refs")
    public ImmutableList<ItemRef> getItemRefs() {
        return itemRefs;
    }

    public void setItemRefs(@Nullable Iterable<ItemRef> itemRefs) {
        if (itemRefs == null) {
            this.itemRefs = null;
            return;
        }
        this.itemRefs = ImmutableList.copyOf(itemRefs);
    }

    public static Container copyTo(Container from, Container to) {
        Content.copyTo(from, to);
        // they're either null or immutable, so straight copy is safe
        to.itemRefs = from.itemRefs;
        to.upcomingContent = from.upcomingContent;
        to.availableContent = from.availableContent;
        to.itemSummaries = from.itemSummaries;
        return to;
    }

    @Override public <T extends Described> T copyTo(T to) {
        if (to instanceof Container) {
            copyTo(this, (Container) to);
            return to;
        }
        return super.copyTo(to);
    }

    @Override public <T extends Described> T copyToPreferNonNull(T to) {
        if (to instanceof Container) {
            copyToPreferNonNull(this, (Container) to);
            return to;
        }
        return super.copyToPreferNonNull(to);
    }

    public static Container copyToPreferNonNull(Container from, Container to) {
        Content.copyToPreferNonNull(from, to);
        to.itemRefs = from.itemRefs == null || from.itemRefs.isEmpty()
                      ? to.itemRefs
                      : from.itemRefs;
        to.upcomingContent = from.upcomingContent == null || from.upcomingContent.isEmpty()
                             ? to.upcomingContent
                             : from.upcomingContent;
        to.availableContent = from.availableContent == null || from.availableContent.isEmpty()
                              ? to.availableContent
                              : from.availableContent;
        to.itemSummaries = from.itemSummaries == null || from.itemSummaries.isEmpty()
                           ? to.itemSummaries
                           : from.itemSummaries;
        return to;
    }

    public abstract <V> V accept(ContainerVisitor<V> visitor);

    public abstract ContainerRef toRef();

    public abstract ContainerSummary toSummary();

    @Override
    public <V> V accept(ContentVisitor<V> visitor) {
        return accept((ContainerVisitor<V>) visitor);
    }

    @Nullable
    public Map<ItemRef, Iterable<BroadcastRef>> getUpcomingContent() {
        return upcomingContent;
    }

    public void setUpcomingContent(@Nullable Map<ItemRef, Iterable<BroadcastRef>> upcomingContent) {
        if (upcomingContent == null) {
            this.upcomingContent = null;
            return;
        }
        this.upcomingContent = ImmutableMap.copyOf(
                Maps.filterValues(
                        Maps.transformValues(
                                upcomingContent,
                                input -> input == null ? null : StreamSupport.stream(input.spliterator(), false)
                                            .filter(BroadcastRef.IS_UPCOMING)
                                            .collect(MoreCollectors.toImmutableList())
                        ),
                        input -> input != null && !input.isEmpty()
                )
        );
    }

    @Nullable
    public Map<ItemRef, Iterable<LocationSummary>> getAvailableContent() {
        return availableContent;
    }

    public void setAvailableContent(@Nullable Map<ItemRef, Iterable<LocationSummary>> availableContent) {
        if (availableContent == null) {
            this.availableContent = null;
            return;
        }
        this.availableContent = ImmutableMap.copyOf(
                Maps.filterValues(
                        Maps.transformValues(
                                availableContent,
                                input -> input == null ? null : StreamSupport.stream(input.spliterator(), false)
                                        .filter(LocationSummary::isAvailable)
                                        .collect(MoreCollectors.toImmutableList())
                        ),
                        input -> input != null && !input.isEmpty()
                )
        );
    }

    @Nullable
    public List<ItemSummary> getItemSummaries() {
        return itemSummaries;
    }

    public void setItemSummaries(@Nullable List<ItemSummary> itemSummaries) {
        if (itemSummaries == null) {
            this.itemSummaries = null;
            return;
        }
        this.itemSummaries = itemSummaries.stream()
                .sorted(ItemSummary.ORDERING)
                .collect(MoreCollectors.toImmutableList());
    }
}
