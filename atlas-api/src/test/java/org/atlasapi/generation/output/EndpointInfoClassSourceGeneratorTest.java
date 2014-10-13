package org.atlasapi.generation.output;

import static org.junit.Assert.assertEquals;

import org.atlasapi.generation.model.EndpointMethodInfo;
import org.atlasapi.generation.model.EndpointTypeInfo;
import org.junit.Test;
import org.springframework.web.bind.annotation.RequestMethod;


public class EndpointInfoClassSourceGeneratorTest {
    
    private final EndpointClassInfoSourceGenerator generator = new EndpointClassInfoSourceGenerator();

    @Test
    public void testCreationOfSetElementCreatesCorrectFieldInfoInstantiation() {
        EndpointMethodInfo methodInfo = new EndpointMethodInfo("path", RequestMethod.GET);
        
        String setElement = generator.setElementFromMethod(methodInfo, "");
        
        assertEquals(expected(methodInfo), setElement);
    }
    
    @Test
    public void testCreationOfModelTypeBasedFields() {
        EndpointTypeInfo typeInfo = EndpointTypeInfo.builder()
                .withKey("key")
                .withDescription("description")
                .withClassName("className")
                .withProducedType("ProducedType")
                .withRootPath("rootPath")
                .build();
        
        String typeBasedFields = generator.generateTypeBasedFields(typeInfo);
        
        assertEquals(expected(typeInfo), typeBasedFields);
    }

    private String expected(EndpointTypeInfo typeInfo) {
        String expectedPattern = new StringBuilder()
                .append("    @Override\n")
                .append("    public String name() {\n")
                .append("        return %s;\n")
                .append("    }\n")
                .append("\n")
                .append("    @Override\n")
                .append("    public String description() {\n")
                .append("        return %s;\n")
                .append("    }\n")
                .append("\n")
                .append("    @Override\n")
                .append("    public String rootPath() {\n")
                .append("        return %s;\n")
                .append("    }\n")
                .toString();
        
        return String.format(
                expectedPattern,
                typeInfo.producedType().toLowerCase(),
                typeInfo.description(),
                typeInfo.rootPath()
        );
    }

    private String expected(EndpointMethodInfo methodInfo) {
        return String.format(
                "new Operation(RequestMethod.%s, %s)", 
                methodInfo.method(), 
                methodInfo.path()
        );
    }

}
