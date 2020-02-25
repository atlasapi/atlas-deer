package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import static org.atlasapi.application.ApplicationAccessRole.REP_ID_SERVICE;

public class RepIdAnnotation extends OutputAnnotation<Content> {

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        String repId = "patata";

        if (!ctxt.getApplication().getAccessRoles().hasRole(REP_ID_SERVICE.getRole())) {
            throw new IllegalArgumentException(
                    "This application does not have access to the Representative ID service. "
                    + "Please ask MB about enabling access to the service.");
        }

        writer.writeField("rep_id", repId);
    }
}
