package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.SourceLicense;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class SourceLicenseWithIdWriter implements EntityListWriter<SourceLicense> {

    private final EntityWriter<Publisher> sourceWriter;

    public SourceLicenseWithIdWriter(SourceIdCodec sourceIdCodec) {
        this.sourceWriter = new SourceWithIdWriter(sourceIdCodec, "source", "source");
    }

    @Override
    public void write(SourceLicense entity, FieldWriter writer,
            OutputContext ctxt) throws IOException {
        writer.writeObject(sourceWriter, entity.getSource(), ctxt);
        writer.writeField("license", entity.getLicense());
    }

    @Override
    public String fieldName(SourceLicense entity) {
        return "source_license";
    }

    @Override
    public String listName() {
        return "source_licenses";
    }

}
