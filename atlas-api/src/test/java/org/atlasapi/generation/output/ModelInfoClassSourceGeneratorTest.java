package org.atlasapi.generation.output;

import org.atlasapi.generation.model.JsonType;
import org.atlasapi.generation.model.ModelMethodInfo;
import org.atlasapi.generation.model.ModelTypeInfo;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ModelInfoClassSourceGeneratorTest {

    private final ModelClassInfoSourceGenerator generator = new ModelClassInfoSourceGenerator();

    @Test
    public void testCreationOfSetElementCreatesCorrectFieldInfoInstantiation() {
        ModelMethodInfo methodInfo = ModelMethodInfo.builder()
                .withName("name")
                .withDescription("description")
                .withIsModelType(true)
                .withIsMultiple(false)
                .withJsonType(JsonType.NUMBER)
                .withType("type")
                .build();

        String setElement = generator.setElementFromMethod(methodInfo, "");

        assertEquals(expected(methodInfo), setElement);
    }

    @Test
    public void testCreationOfModelTypeBasedFields() {
        ModelTypeInfo typeInfo = ModelTypeInfo.builder()
                .withKey("key")
                .withDescription("description")
                .withOutputClassName("outputClass")
                .withParsedClass("parsedClass")
                .build();

        String typeBasedFields = generator.generateTypeBasedFields(typeInfo);

        assertEquals(expected(typeInfo), typeBasedFields);
    }

    @Test
    public void testFullClassCreation() {
        ModelTypeInfo typeInfo = ModelTypeInfo.builder()
                .withKey("key")
                .withDescription("description")
                .withOutputClassName("outputClass")
                .withParsedClass("parsedClass")
                .build();

        ModelMethodInfo methodInfo = ModelMethodInfo.builder()
                .withName("name")
                .withDescription("description")
                .withIsModelType(true)
                .withIsMultiple(false)
                .withJsonType(JsonType.NUMBER)
                .withType("type")
                .build();

        String classString = generator.processType(typeInfo, ImmutableList.of(methodInfo));

        assertEquals(expectedClassString(typeInfo, methodInfo), classString);
    }

    private String expected(ModelTypeInfo typeInfo) {
        String expectedPattern = new StringBuilder()
                .append("    @Override\n")
                .append("    public String key() {\n")
                .append("        return %s;\n")
                .append("    }\n")
                .append("\n")
                .append("    @Override\n")
                .append("    public String description() {\n")
                .append("        return %s;\n")
                .append("    }\n")
                .append("\n")
                .append("    @Override\n")
                .append("    public Class<?> describedType() {\n")
                .append("        return %s;\n")
                .append("    }\n")
                .toString();

        return String.format(
                expectedPattern,
                typeInfo.key(),
                typeInfo.description(),
                typeInfo.parsedClass()
        );
    }

    private String expected(ModelMethodInfo methodInfo) {
        String expectedPattern = new StringBuilder()
                .append("FieldInfo.builder()\n")
                .append("    .withName(\"%s\")\n")
                .append("    .withDescription(\"%s\")\n")
                .append("    .withType(\"%s\")\n")
                .append("    .withIsMultiple(%s)\n")
                .append("    .withIsModelType(%s)\n")
                .append("    .withJsonType(JsonType.%s)\n")
                .append("    .build()")
                .toString();

        return String.format(
                expectedPattern,
                methodInfo.name(),
                methodInfo.description(),
                methodInfo.type(),
                methodInfo.isMultiple().toString(),
                methodInfo.isModelType().toString(),
                methodInfo.jsonType().name()
        );
    }

    private String expectedClassString(ModelTypeInfo typeInfo, ModelMethodInfo methodInfo) {
        String expectedClassPattern = new StringBuilder()
                .append("package org.atlasapi.generation.generated.model;\n")
                .append("\n")
                .append("import java.util.Set;\n")
                .append("import com.google.common.collect.ImmutableSet;\n")
                .append("import org.atlasapi.generation.model.ModelClassInfo;\n")
                .append("import org.atlasapi.generation.model.FieldInfo;\n")
                .append("import org.atlasapi.generation.model.JsonType;\n")
                .append("\n")
                .append("\n")
                .append("public class %s implements ModelClassInfo {\n")
                .append("\n")
                .append("    private static final Set<FieldInfo> fields = ImmutableSet.<FieldInfo>builder()\n")
                .append("            .add(\n")
                .append("                FieldInfo.builder()\n")
                .append("                    .withName(\"%s\")\n")
                .append("                    .withDescription(\"%s\")\n")
                .append("                    .withType(\"%s\")\n")
                .append("                    .withIsMultiple(%s)\n")
                .append("                    .withIsModelType(%s)\n")
                .append("                    .withJsonType(JsonType.%s)\n")
                .append("                    .build()\n")
                .append("            )\n")
                .append("            .build();\n")
                .append("\n")
                .append("    @Override\n")
                .append("    public Set<FieldInfo> fields() {\n")
                .append("        return fields;\n")
                .append("    }\n")
                .append("\n")
                .append("    @Override\n")
                .append("    public String key() {\n")
                .append("        return %s;\n")
                .append("    }\n")
                .append("\n")
                .append("    @Override\n")
                .append("    public String description() {\n")
                .append("        return %s;\n")
                .append("    }\n")
                .append("\n")
                .append("    @Override\n")
                .append("    public Class<?> describedType() {\n")
                .append("        return %s;\n")
                .append("    }\n")
                .append("\n")
                .append("}\n")
                .toString();

        return String.format(
                expectedClassPattern,
                typeInfo.className(),
                methodInfo.name(),
                methodInfo.description(),
                methodInfo.type(),
                methodInfo.isMultiple().toString(),
                methodInfo.isModelType().toString(),
                methodInfo.jsonType().name(),
                typeInfo.key(),
                typeInfo.description(),
                typeInfo.parsedClass()
        );
    }

}
