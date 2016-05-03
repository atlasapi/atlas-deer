package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class ApplicationSourcesWriter implements EntityListWriter<ApplicationSources> {

    private final EntityListWriter<SourceReadEntry> readsWriter;
    private final EntityListWriter<Publisher> writesWriter;

    public ApplicationSourcesWriter(SourceIdCodec sourceIdCodec) {
        readsWriter = new ApplicationSourcesReadsWriter(sourceIdCodec);
        writesWriter = new SourceWithIdWriter(sourceIdCodec, "writes", "writes");
    }

    @Override
    public void write(ApplicationSources entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {

        writer.writeField("precedence", entity.isPrecedenceEnabled());
        writer.writeField("image_precedence_enabled", entity.imagePrecedenceEnabled());
        writer.writeList(readsWriter, entity.getReads(), ctxt);
        writer.writeList(writesWriter, entity.getWrites(), ctxt);
    }

    @Override
    public String fieldName(ApplicationSources entity) {
        return "sources";
    }

    @Override
    public String listName() {
        return "sources";
    }

}
