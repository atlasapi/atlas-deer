package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.content.BlackoutRestriction;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class BlackoutRestrictionWriter implements EntityWriter<BlackoutRestriction> {

    @Override
    public void write(@Nonnull BlackoutRestriction entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        writer.writeField("all", entity.getAll());
    }

    @Nonnull
    @Override
    public String fieldName(BlackoutRestriction entity) {
        return "blackout_restriction";
    }
}
