package org.atlasapi.content.v2;

import java.util.Map;
import java.util.Set;

import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.Broadcast;
import org.atlasapi.content.v2.model.udt.ContainerSummary;
import org.atlasapi.content.v2.model.udt.ItemRef;
import org.atlasapi.content.v2.model.udt.ItemRefAndBroadcastRefs;
import org.atlasapi.content.v2.model.udt.ItemRefAndItemSummary;
import org.atlasapi.content.v2.model.udt.ItemRefAndLocationSummaries;
import org.atlasapi.content.v2.model.udt.Ref;
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
    Statement addBroadcastToContent(
            @Param("id") Long id,
            @Param("bc") Map<String, Broadcast> broadcasts
    );

    @Query("UPDATE content_v2 SET "
            + "item_refs = item_refs + :refs, "
            + "upcoming = upcoming + :upcoming, "
            + "available = available + :available "
            + "WHERE id = :id")
    Statement addItemRefsToContainer(
            @Param("id") Long id,
            @Param("refs") Map<Ref, ItemRef> itemRefs,
            @Param("upcoming") Map<Ref, ItemRefAndBroadcastRefs> upcoming,
            @Param("available") Map<Ref, ItemRefAndLocationSummaries> available
    );

    @Query("UPDATE content_v2 SET "
            + "item_refs = item_refs - :refs, "
            + "upcoming = upcoming - :upcoming, "
            + "available = available - :available "
            + "WHERE id = :id")
    Statement removeItemRefsFromContainer(
            @Param("id") Long id,
            @Param("refs") Set<Ref> itemRefs,
            @Param("upcoming") Set<Ref> upcoming,
            @Param("available") Set<Ref> available
    );

    @Query("UPDATE content_v2 SET item_summaries = item_summaries + :summaries WHERE id = :id")
    Statement addItemSummariesToContainer(
            @Param("id") Long id,
            @Param("summaries") Map<Ref, ItemRefAndItemSummary> itemSummaries
    );

    @Query("UPDATE content_v2 SET item_summaries = item_summaries - :summaries WHERE id = :id")
    Statement removeItemSummariesFromContainer(
            @Param("id") Long id,
            @Param("summaries") Set<Ref> itemSummaries
    );

    @Query("UPDATE content_v2 SET series_refs = series_refs + :refs WHERE id = :id")
    Statement addSeriesRefToBrand(
            @Param("id") Long brandId,
            @Param("refs") Map<Ref, SeriesRef> seriesRefs
    );

    @Query("UPDATE content_v2 SET series_refs = series_refs - :refs WHERE id = :id")
    Statement removeSeriesRefFromBrand(
            @Param("id") Long brandId,
            @Param("refs") Set<Ref> seriesRefs
    );

    @Query("UPDATE content_v2 SET container_summary = :summary WHERE id = :id")
    Statement updateContainerSummaryInChild(
            @Param("id") Long childId,
            @Param("summary") ContainerSummary containerSummary
    );
}
