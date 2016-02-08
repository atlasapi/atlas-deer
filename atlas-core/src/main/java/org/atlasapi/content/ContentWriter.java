package org.atlasapi.content;

import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;

import com.google.common.base.Optional;

public interface ContentWriter {

    <C extends Content> WriteResult<C, Content> writeContent(C content) throws WriteException;

    void writeBroadcast(
            ItemRef item,
            Optional<ContainerRef> containerRef,
            Optional<SeriesRef> seriesRef,
            Broadcast broadcast
    );

}
