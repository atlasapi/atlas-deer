package org.atlasapi.generation.output;

import java.util.Set;

import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.JsonType;
import org.atlasapi.generation.model.ModelClassInfo;
import org.atlasapi.generation.model.ModelMethodInfo;
import org.atlasapi.generation.model.ModelTypeInfo;

import com.google.common.collect.ImmutableSet;

public class ModelClassInfoSourceGenerator
        extends AbstractSourceGenerator<ModelTypeInfo, ModelMethodInfo> {

    @Override
    public Set<Class<?>> importedClasses() {
        return ImmutableSet.<Class<?>>builder()
                .add(Set.class)
                .add(ImmutableSet.class)
                .add(ModelClassInfo.class)
                .add(FieldInfo.class)
                .add(JsonType.class)
                .build();
    }

    @Override
    public Class<?> inheritedInterfaceName() {
        return ModelClassInfo.class;
    }

    @Override
    public String setName() {
        return "fields";
    }

    @Override
    public Class<?> setType() {
        return FieldInfo.class;
    }

    // TODO fix this and tidy it up (remove all these ghastly tabs, for one thing)
    // need to define boundaries between this and where it's used more clearly
    // can the logic that parses params from methods/types be pulled out from the source creation code?
    // if so, what's the intermediate form?
    @Override
    public String setElementFromMethod(ModelMethodInfo method, String indent) {
        StringBuilder setElement = new StringBuilder();

        setElement.append(indent);
        setElement.append("FieldInfo.builder()\n");

        setElement.append(createBuilderMethod(
                indent + TAB,
                "withName",
                addQuotesToString(method.name())
        ));
        setElement.append(createBuilderMethod(
                indent + TAB,
                "withDescription",
                addQuotesToString(method.description())
        ));
        setElement.append(createBuilderMethod(
                indent + TAB,
                "withType",
                addQuotesToString(method.type())
        ));
        setElement.append(createBuilderMethod(
                indent + TAB,
                "withIsMultiple",
                method.isMultiple().toString()
        ));
        setElement.append(createBuilderMethod(
                indent + TAB,
                "withIsModelType",
                method.isModelType().toString()
        ));
        setElement.append(createBuilderMethod(
                indent + TAB,
                "withJsonType",
                "JsonType." + method.jsonType().name()
        ));

        setElement.append(indent + TAB);
        setElement.append(".build()");

        return setElement.toString();
    }

    private String createBuilderMethod(String whitespace, String methodName, String paramValue) {
        return new StringBuilder()
                .append(whitespace)
                .append(".")
                .append(methodName)
                .append("(")
                .append(paramValue)
                .append(")\n")
                .toString();
    }

    @Override
    public String generateTypeBasedFields(ModelTypeInfo typeInfo) {
        return new StringBuilder()
                .append(formatOverriddenMethod("key", "String", typeInfo.key()))
                .append(NEWLINE)
                .append(formatOverriddenMethod("description", "String", typeInfo.description()))
                .append(NEWLINE)
                .append(formatOverriddenMethod("describedType", "Class<?>", typeInfo.parsedClass()))
                .toString();
    }
}
