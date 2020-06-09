package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Described;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.LocalizedTitlesWriter;

public class LocalizedTitlesAnnotation extends OutputAnnotation<Described>{

    public LocalizedTitlesAnnotation() {
        super();
    }

    @Override
    public void write(Described entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(new LocalizedTitlesWriter(), entity.getLocalizedTitles(), ctxt);
    }

}
