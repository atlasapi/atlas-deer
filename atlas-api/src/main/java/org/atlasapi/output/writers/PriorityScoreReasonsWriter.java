package org.atlasapi.output.writers;

import org.atlasapi.content.PriorityScoreReasons;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import javax.annotation.Nonnull;
import java.io.IOException;

public class PriorityScoreReasonsWriter implements EntityWriter<PriorityScoreReasons> {

    @Override
    public void write(@Nonnull PriorityScoreReasons entity, @Nonnull FieldWriter writer,
                      @Nonnull OutputContext ctxt) throws IOException {
        writer.writeList("positive", "positive", entity.getPositive(), ctxt);
        writer.writeList("negative", "negative", entity.getNegative(), ctxt);
    }

    @Nonnull
    @Override
    public String fieldName(PriorityScoreReasons entity) {
        return "priority_reasons";
    }
}
