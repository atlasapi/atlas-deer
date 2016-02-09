package org.atlasapi.output.writers;

import java.io.IOException;

import org.atlasapi.content.Certificate;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public final class CertificateWriter implements EntityListWriter<Certificate> {

    @Override
    public void write(Certificate entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("classification", entity.classification());
        writer.writeField("code", entity.country().code());
    }

    @Override
    public String listName() {
        return "certificates";
    }

    @Override
    public String fieldName(Certificate entity) {
        return "certificate";
    }
}