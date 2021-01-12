package org.atlasapi.schedule;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.Series;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;

import javax.annotation.Nullable;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleHierarchy {

    public static final ScheduleHierarchy itemOnly(ItemAndBroadcast item) {
        return new ScheduleHierarchy(item, (Container) null, null);
    }

    public static final ScheduleHierarchy seriesAndItem(Series series, ItemAndBroadcast item) {
        if (item.getItem() instanceof Episode) {
            return new ScheduleHierarchy(item, series, series);
        }
        return new ScheduleHierarchy(item, series, null);
    }

    public static final ScheduleHierarchy brandAndItem(Brand brand, ItemAndBroadcast item) {
        return new ScheduleHierarchy(item, brand, null);
    }

    public static final ScheduleHierarchy brandSeriesAndItem(Brand brand, Series series,
            ItemAndBroadcast item) {
        return new ScheduleHierarchy(item, brand, series);
    }

    private final ItemAndBroadcast itemAndBroadcast;
    private final Optional<Container> primaryContainer;
    private final Optional<Series> possibleSeries;

    public ScheduleHierarchy(ItemAndBroadcast itemAndBroadcast,
            @Nullable Container primaryContainer,
            @Nullable Series secondaryContainer) {
        this(
                checkNotNull(itemAndBroadcast),
                Optional.fromNullable(primaryContainer),
                Optional.fromNullable(secondaryContainer)
        );
    }

    @SuppressWarnings("unchecked")
    // http://stackoverflow.com/questions/7848789/how-to-use-guava-optional-as-naturally-covariant-object
    public ScheduleHierarchy(ItemAndBroadcast itemAndBroadcast,
            Optional<? extends Container> primaryContainer, Optional<Series> secondaryContainer) {
        checkArgument(
                !secondaryContainer.isPresent() || itemAndBroadcast.getItem() instanceof Episode,
                "Only an Episode can have a secondary container"
        );
        this.itemAndBroadcast = checkNotNull(itemAndBroadcast);
        this.primaryContainer = checkNotNull((Optional<Container>) primaryContainer);
        this.possibleSeries = checkNotNull(secondaryContainer);
    }

    public ItemAndBroadcast getItemAndBroadcast() {
        return this.itemAndBroadcast;
    }

    public Optional<Container> getPrimaryContainer() {
        return this.primaryContainer;
    }

    public Optional<Series> getPossibleSeries() {
        return this.possibleSeries;
    }

    List<WriteResult<? extends Content, Content>> writeTo(ContentStore store)
            throws WriteException {
        List<WriteResult<? extends Content, Content>> results = Lists.newArrayListWithCapacity(3);
        WriteResult<Container, Content> primaryContainerResult = null;
        if (primaryContainer.isPresent()) {
            primaryContainerResult = store.writeContent(primaryContainer.get());
            results.add(primaryContainerResult);
        }
        WriteResult<Series, Content> secondaryContainerResult = null;
        if (possibleSeries.isPresent()) {
            if (primaryContainerResult != null
                    && primaryContainerResult.getResource() instanceof Brand) {
                possibleSeries.get().setBrand((Brand) primaryContainer.get());
            }
            secondaryContainerResult = store.writeContent(possibleSeries.get());
            results.add(secondaryContainerResult);
        }
        Item item = itemAndBroadcast.getItem();
        if (primaryContainerResult != null) {
            item.setContainer(primaryContainerResult.getResource());
        }
        if (secondaryContainerResult != null && item instanceof Episode) {
            ((Episode) item).setSeries(secondaryContainerResult.getResource());
        }
        results.add(store.writeContent(item));
        return results;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof ScheduleHierarchy) {
            ScheduleHierarchy other = (ScheduleHierarchy) that;
            return itemAndBroadcast.equals(other.itemAndBroadcast)
                    && primaryContainer.equals(other.primaryContainer)
                    && possibleSeries.equals(other.possibleSeries);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return itemAndBroadcast.hashCode();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass()).omitNullValues()
                .add("item", itemAndBroadcast)
                .add("primary", primaryContainer.orNull())
                .add("secondary", possibleSeries.orNull())
                .toString();
    }

}
