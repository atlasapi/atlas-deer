package org.atlasapi.content;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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

    @FieldName("item_refs")
    public ImmutableList<ItemRef> getItemRefs() {
        return itemRefs;
    }

    public void setItemRefs(Iterable<ItemRef> itemRefs) {
        this.itemRefs = ImmutableList.copyOf(itemRefs);
    }

    public final static <T extends Item> void copyTo(Container from, Container to) {
        Content.copyTo(from, to);
        to.itemRefs = ImmutableList.copyOf(from.itemRefs);
    }

    public abstract <V> V accept(ContainerVisitor<V> visitor);

    public abstract ContainerRef toRef();

    public abstract ContainerSummary toSummary();

    @Override
    public <V> V accept(ContentVisitor<V> visitor) {
        return accept((ContainerVisitor<V>) visitor);
    }

    public Map<ItemRef, Iterable<BroadcastRef>> getUpcomingContent() {
        return upcomingContent;
    }

    public void setUpcomingContent(Map<ItemRef, Iterable<BroadcastRef>> upcomingContent) {
        this.upcomingContent = ImmutableMap.copyOf(
                Maps.filterValues(
                        Maps.transformValues(
                                upcomingContent,
                                new Function<Iterable<BroadcastRef>, Iterable<BroadcastRef>>() {

                                    @Nullable
                                    @Override
                                    public Iterable<BroadcastRef> apply(
                                            Iterable<BroadcastRef> input) {
                                        return ImmutableList.copyOf(input)
                                                .stream()
                                                .filter(BroadcastRef.IS_UPCOMING)
                                                .collect(Collectors.toList());
                                    }

                                }
                        ),
                        input -> !Iterables.isEmpty(input)
                )
        );
    }

    public Map<ItemRef, Iterable<LocationSummary>> getAvailableContent() {
        return availableContent;
    }

    public void setAvailableContent(Map<ItemRef, Iterable<LocationSummary>> availableContent) {
        this.availableContent = ImmutableMap.copyOf(
                Maps.filterValues(
                        Maps.transformValues(
                                availableContent,
                                new Function<Iterable<LocationSummary>, Iterable<LocationSummary>>() {

                                    @Nullable
                                    @Override
                                    public Iterable<LocationSummary> apply(
                                            Iterable<LocationSummary> input) {
                                        return ImmutableList.copyOf(input)
                                                .stream()
                                                .filter(LocationSummary::isAvailable)
                                                .collect(Collectors.toList());
                                    }

                                }
                        ),
                        input -> !Iterables.isEmpty(input)
                )
        );
    }

    public List<ItemSummary> getItemSummaries() {
        return itemSummaries;
    }

    public void setItemSummaries(List<ItemSummary> itemSummaries) {
        this.itemSummaries = itemSummaries.stream()
                .sorted(ItemSummary.ORDERING)
                .collect(MoreCollectors.toList());
    }
}
