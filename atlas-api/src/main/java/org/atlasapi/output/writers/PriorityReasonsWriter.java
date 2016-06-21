package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.content.PriorityScoreReasons;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class PriorityReasonsWriter implements EntityWriter<PriorityScoreReasons> {

    @Nonnull
    @Override
    public String fieldName(PriorityScoreReasons entity) {
        return "priority_reasons";
    }

    @Override
    public void write(@Nonnull PriorityScoreReasons entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        writer.writeList(
                "positive",
                "positive",
                entity.getPositive(),
                ctxt
        );

        writer.writeList(
                "negative",
                "negative",
                entity.getNegative(),
                ctxt
        );
    }
}
