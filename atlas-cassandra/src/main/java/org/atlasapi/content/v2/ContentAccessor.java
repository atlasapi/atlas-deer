package org.atlasapi.content.v2;

import org.atlasapi.content.v2.model.Content;

import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;

@Accessor
public interface ContentAccessor {

    @Query("SELECT * FROM content_v2 WHERE id = :id")
    ListenableFuture<Content> getContent(@Param("id") Long id);
}
