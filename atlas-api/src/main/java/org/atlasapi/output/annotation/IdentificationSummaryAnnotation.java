package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.entity.Identified;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.IdSummaryWriter;

public class IdentificationSummaryAnnotation extends OutputAnnotation<Identified> {

    private IdSummaryWriter idSummaryWriter;

    public IdentificationSummaryAnnotation() {
        super();
        this.idSummaryWriter = IdSummaryWriter.create();
    }

    @Override
    public void write(Identified entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        idSummaryWriter.write(entity, writer, ctxt);
    }
}
