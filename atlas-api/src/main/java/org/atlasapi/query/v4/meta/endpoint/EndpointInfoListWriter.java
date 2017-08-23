package org.atlasapi.query.v4.meta.endpoint;

import java.io.IOException;
import java.util.List;

import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;

import static com.google.common.base.Preconditions.checkNotNull;

public class EndpointInfoListWriter implements EntityListWriter<EndpointClassInfo> {

    private final AnnotationRegistry<EndpointClassInfo, EndpointClassInfo> annotationRegistry;

    public EndpointInfoListWriter(AnnotationRegistry<EndpointClassInfo, EndpointClassInfo> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }

    @Override
    public void write(EndpointClassInfo entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        ctxt.startResource(Resource.ENDPOINT_INFO);
        List<OutputAnnotation<? super EndpointClassInfo, EndpointClassInfo>> annotations = ctxt
                .getAnnotations(annotationRegistry);
        for (OutputAnnotation<? super EndpointClassInfo, EndpointClassInfo> annotation : annotations) {
            annotation.write(entity, writer, ctxt);
        }
        ctxt.endResource();
    }

    @Override
    public String listName() {
        return "resources";
    }

    @Override
    public String fieldName(EndpointClassInfo entity) {
        return "resource";
    }

}
