package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.License;
import org.atlasapi.output.OutputContext;

public class LicenseWriter implements EntityWriter<Object> {

    private final License license;

    public LicenseWriter(License license) {
        this.license = license;
    }

    @Override
    public void write(@Nonnull Object object, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        writer.writeField("text", license.getText());
    }

    @Nonnull
    @Override
    public String fieldName(Object entity) {
        return "terms_and_conditions";
    }
}
