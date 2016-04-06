package org.atlasapi.content.v2;

import java.util.Set;

import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.ContainerSummary;
import org.atlasapi.content.v2.model.udt.ItemRef;
import org.atlasapi.content.v2.model.udt.ItemSummary;
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

    @Query("UPDATE content_v2 SET toclu = :now WHERE id = :id")
    Statement setLastUpdated(@Param("id") Long id, @Param("now") Instant now);

    @Query("UPDATE content_v2 SET itr = itr + :refs, its = its + :summaries WHERE id = :id")
    Statement addItemRefsToContainer(
            @Param("id") Long id,
            @Param("refs") Set<ItemRef> itemRefs,
            @Param("summaries") Set<ItemSummary> itemSummaries
    );

    @Query("UPDATE content_v2 SET ser = ser + :refs WHERE id = :id")
    Statement addSeriesRefToBrand(
            @Param("id") Long brandId,
            @Param("refs") Set<SeriesRef> seriesRefs
    );

    @Query("UPDATE content_v2 SET cns = :summary WHERE id = :id")
    Statement updateContainerSummaryInChild(
            @Param("id") Long childId,
            @Param("summary") ContainerSummary containerSummary
    );
}
