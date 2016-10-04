package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.entity.Identified;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.IdSummaryWriter;

public class IdentificationSummaryAnnotation extends OutputAnnotation<Identified> {

    private final IdSummaryWriter idSummaryWriter;

    private IdentificationSummaryAnnotation(IdSummaryWriter idSummaryWriter) {
        super();
        this.idSummaryWriter = idSummaryWriter;
    }

    public static IdentificationSummaryAnnotation create(IdSummaryWriter idSummaryWriter) {
        return new IdentificationSummaryAnnotation(idSummaryWriter);
    }

    @Override
    public void write(Identified entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        idSummaryWriter.write(entity, writer, ctxt);
    }
}
