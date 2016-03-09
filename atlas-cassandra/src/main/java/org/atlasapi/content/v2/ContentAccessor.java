package org.atlasapi.content.v2;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;

@Accessor
public interface ContentAccessor {

    @Query("SELECT * FROM content_v2 WHERE id = :id")
    ResultSetFuture getContent(@Param("id") Long id);
}
