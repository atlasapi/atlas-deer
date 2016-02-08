package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.EndUserLicense;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.Resource;

public class EndUserLicenseListWriter implements EntityListWriter<EndUserLicense> {

    public EndUserLicenseListWriter() {
    }

    @Override
    public void write(EndUserLicense entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        ctxt.startResource(Resource.END_USER_LICENSE);
        writer.writeField("license", entity.getLicense());
        ctxt.endResource();
    }

    @Override
    public String fieldName(EndUserLicense entity) {
        return "license";
    }

    @Override
    public String listName() {
        return "licenses";
    }

}
