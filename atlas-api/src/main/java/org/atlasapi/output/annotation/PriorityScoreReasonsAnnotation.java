package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.PriorityReasonsWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class PriorityScoreReasonsAnnotation extends OutputAnnotation<Content, Content> {

    private final PriorityReasonsWriter priorityReasonsWriter;

    public PriorityScoreReasonsAnnotation(PriorityReasonsWriter priorityReasonsWriter) {
        this.priorityReasonsWriter = checkNotNull(priorityReasonsWriter);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeObject(priorityReasonsWriter, "priority_reasons", entity.getPriority().getReasons(), ctxt);
    }
}