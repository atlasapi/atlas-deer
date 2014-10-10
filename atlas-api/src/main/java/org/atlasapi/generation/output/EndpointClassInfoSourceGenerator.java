package org.atlasapi.generation.output;

import java.util.Set;

import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.generation.model.EndpointMethodInfo;
import org.atlasapi.generation.model.EndpointTypeInfo;
import org.atlasapi.generation.model.Operation;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.collect.ImmutableSet;

public class EndpointClassInfoSourceGenerator extends AbstractSourceGenerator<EndpointTypeInfo, EndpointMethodInfo> {
	
    @Override
    public String setName() {
        return "operations";
    }

    @Override
    public Class<?> setType() {
        return Operation.class;
    }

    @Override
    public String setElementFromMethod(EndpointMethodInfo method, String indent) {
        return String.format("%snew Operation(RequestMethod.%s, %s)", indent, method.method(), method.path());
    }

    @Override
    public Set<Class<?>> importedClasses() {
        return ImmutableSet.of(
                Set.class,
                ImmutableSet.class,
                EndpointClassInfo.class,
                Operation.class,
                RequestMethod.class
        );
    }

    @Override
    public Class<?> inheritedInterfaceName() {
        return EndpointClassInfo.class;
    }

    @Override
    public String generateTypeBasedFields(EndpointTypeInfo typeInfo) {
        return new StringBuilder()
        .append(formatOverriddenMethod("name", "String", typeInfo.producedType().toLowerCase()))
        .append(NEWLINE)
        .append(formatOverriddenMethod("description", "String", typeInfo.description()))
        .append(NEWLINE)
        .append(formatOverriddenMethod("rootPath", "String", typeInfo.rootPath()))
        .toString();
    }
}
