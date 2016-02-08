package org.atlasapi.output;

import java.io.IOException;

import org.atlasapi.query.common.ContextualQueryResult;

public interface ContextualResultWriter<CONTEXT, RESOURCE> {

    void write(ContextualQueryResult<CONTEXT, RESOURCE> result, ResponseWriter responseWriter)
            throws IOException;

}
