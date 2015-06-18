package org.atlasapi.output.annotation;

import org.atlasapi.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import java.io.IOException;

/**
 * Marker for {@link BroadcastsAnnotation} to only write broadcasts that have not yet broadcast.
 */
public class UpcomingBroadcastsAnnotation extends OutputAnnotation<Content> {

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        //no op
    }
}
