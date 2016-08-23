package org.atlasapi.content.v2;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.Broadcast;
import org.atlasapi.content.v2.model.udt.BroadcastRef;
import org.atlasapi.content.v2.model.udt.ContainerSummary;
import org.atlasapi.content.v2.model.udt.ItemRef;
import org.atlasapi.content.v2.model.udt.ItemSummary;
import org.atlasapi.content.v2.model.udt.LocationSummary;
import org.atlasapi.content.v2.model.udt.SeriesRef;

import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.Instant;

@Accessor
public interface ContentAccessor {

    @Query("SELECT * FROM content_v2 WHERE id = :id")
    ListenableFuture<Content> getContent(@Param("id") Long id);

    @Query("UPDATE content_v2 SET this_or_child_last_updated = :now WHERE id = :id")
    Statement setLastUpdated(@Param("id") Long id, @Param("now") Instant now);

    @Query("UPDATE content_v2 SET broadcasts = broadcasts + :bc WHERE id = :id")
    Statement addBroadcastToContent(@Param("id") Long id, @Param("bc") Set<Broadcast> broadcasts);

    @Query("UPDATE content_v2 SET "
            + "item_refs = item_refs + :refs, "
            + "upcoming = upcoming + :upcoming, "
            + "available = available + :available "
            + "WHERE id = :id")
    Statement addItemRefsToContainer(
            @Param("id") Long id,
            @Param("refs") Set<ItemRef> itemRefs,
            @Param("upcoming") Map<ItemRef, List<BroadcastRef>> upcoming,
            @Param("available") Map<ItemRef, List<LocationSummary>> available
    );

    @Query("UPDATE content_v2 SET "
            + "item_refs = item_refs - :refs, "
            + "upcoming = upcoming - :upcoming, "
            + "available = available - :available "
            + "WHERE id = :id")
    Statement removeItemRefsFromContainer(
            @Param("id") Long id,
            @Param("refs") Set<ItemRef> itemRefs,
            @Param("upcoming") Set<ItemRef> upcoming,
            @Param("available") Set<ItemRef> available
    );

    @Query("UPDATE content_v2 SET item_summaries = item_summaries + :summaries WHERE id = :id")
    Statement addItemSummariesToContainer(
            @Param("id") Long id,
            @Param("summaries") Set<ItemSummary> itemSummaries
    );

    @Query("UPDATE content_v2 SET item_summaries = item_summaries - :summaries WHERE id = :id")
    Statement removeItemSummariesFromContainer(
            @Param("id") Long id,
            @Param("summaries") Set<ItemSummary> itemSummaries
    );

    @Query("UPDATE content_v2 SET series_refs = series_refs + :refs WHERE id = :id")
    Statement addSeriesRefToBrand(
            @Param("id") Long brandId,
            @Param("refs") Set<SeriesRef> seriesRefs
    );

    @Query("UPDATE content_v2 SET series_refs = series_refs - :refs WHERE id = :id")
    Statement removeSeriesRefFromBrand(
            @Param("id") Long brandId,
            @Param("refs") Set<SeriesRef> seriesRefs
    );

    @Query("UPDATE content_v2 SET container_summary = :summary WHERE id = :id")
    Statement updateContainerSummaryInChild(
            @Param("id") Long childId,
            @Param("summary") ContainerSummary containerSummary
    );
}
