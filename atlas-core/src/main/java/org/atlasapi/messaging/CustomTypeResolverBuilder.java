package org.atlasapi.messaging;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;

public class CustomTypeResolverBuilder {
    public static class AllowPrimitives extends StdTypeResolverBuilder {
        @Override
        protected boolean allowPrimitiveTypes(MapperConfig<?> config,
                JavaType baseType) {
            return true;
        }
    }
}
