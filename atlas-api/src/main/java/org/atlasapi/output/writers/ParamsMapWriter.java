package org.atlasapi.output.writers;

import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

public class ParamsMapWriter implements EntityWriter<Map<String, String[]>> {

    @Override
    public void write(@Nonnull Map<String, String[]> params, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {
        for (Map.Entry<String, String[]> param : params.entrySet()) {

            writer.writeField(param.getKey(), getFirstOrEmptyString(param.getValue()));
        }

    }

    @Nonnull
    @Override
    public String fieldName(Map<String, String[]> entity) {
        return "params";
    }
    
    private String getFirstOrEmptyString(String[] array) {
        if(array.length > 0) {
            return array[0];
        } else {
            return "";
        }
    }
}
