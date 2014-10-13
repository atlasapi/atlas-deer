package org.atlasapi.generation.output;

import static org.junit.Assert.assertEquals;

import org.atlasapi.generation.model.JsonType;
import org.atlasapi.generation.model.ModelMethodInfo;
import org.atlasapi.generation.model.ModelTypeInfo;
import org.atlasapi.generation.output.ModelClassInfoSourceGenerator;
import org.junit.Test;


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

}
