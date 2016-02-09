package org.atlasapi.output.writers;

import java.io.IOException;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;

import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class RequestWriter implements EntityWriter<HttpServletRequest> {

    @Override
    public void write(@Nonnull HttpServletRequest request, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        writer.writeField("path", request.getServletPath());
        Map<String, String[]> params = request.getParameterMap();
        writer.writeObject(new ParamsMapWriter(), "parameters", params, ctxt);

    }

    @Nonnull
    @Override
    public String fieldName(HttpServletRequest entity) {
        return "request";
    }
}
