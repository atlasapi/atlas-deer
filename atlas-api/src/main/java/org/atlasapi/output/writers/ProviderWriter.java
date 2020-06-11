package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.content.Provider;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class ProviderWriter implements EntityWriter<Provider> {

    @Override
    public void write(@Nonnull Provider entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {

        writer.writeField("name", entity.getName());
        writer.writeField("icon_url", entity.getIconUrl());
    }

    @Nonnull
    @Override
    public String fieldName(Provider entity) {
        return "provider";
    }
}
