package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ClipsWriter;

public class ClipsAnnotation extends OutputAnnotation<Content> {

    private final ClipsWriter clipsWriter;

    public ClipsAnnotation(LocationsAnnotation locationsAnnotation) {
        super();
        clipsWriter = new ClipsWriter(locationsAnnotation);
    }

    @Override
    public void write(Content entity, FieldWriter format, OutputContext ctxt) throws IOException {
        format.writeList(clipsWriter, entity.getClips(), ctxt);
    }

}
