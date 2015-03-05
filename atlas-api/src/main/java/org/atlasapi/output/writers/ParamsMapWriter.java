package org.atlasapi.output.writers;

import com.google.common.collect.Iterables;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class ParamsMapWriter implements EntityWriter<Map<String, String[]>> {

    @Override
    public void write(@Nonnull Map<String, String[]> params, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {
        for (Map.Entry<String, String[]> param : params.entrySet()) {

            writer.writeField(
                    param.getKey(),
                    Iterables.getFirst(Arrays.asList(param.getValue()), "")
            );
        }
    }

    @Nonnull
    @Override
    public String fieldName(Map<String, String[]> entity) {
        return "params";
    }
}
