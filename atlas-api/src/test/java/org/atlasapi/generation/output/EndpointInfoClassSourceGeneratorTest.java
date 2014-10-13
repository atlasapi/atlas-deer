package org.atlasapi.generation.output;

import static org.junit.Assert.assertEquals;

import org.atlasapi.generation.model.EndpointMethodInfo;
import org.atlasapi.generation.model.EndpointTypeInfo;
import org.junit.Test;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.collect.ImmutableList;


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
    
    @Test
    public void testFullClassCreation() {
        EndpointTypeInfo typeInfo = EndpointTypeInfo.builder()
                .withKey("key")
                .withDescription("description")
                .withClassName("className")
                .withProducedType("ProducedType")
                .withRootPath("rootPath")
                .build();
        
        EndpointMethodInfo methodInfo = new EndpointMethodInfo("path", RequestMethod.GET);
        
        String classString = generator.processType(typeInfo, ImmutableList.of(methodInfo));
        
        assertEquals(expectedClassString(typeInfo, methodInfo), classString);
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

    private String expectedClassString(EndpointTypeInfo typeInfo, EndpointMethodInfo methodInfo) {
        String expectedClassPattern = new StringBuilder()
                .append("package org.atlasapi.generation.generated.endpoints;\n")
                .append("\n")
                .append("import java.util.Set;\n")
                .append("import com.google.common.collect.ImmutableSet;\n")
                .append("import org.atlasapi.generation.model.EndpointClassInfo;\n")
                .append("import org.atlasapi.generation.model.Operation;\n")
                .append("import org.springframework.web.bind.annotation.RequestMethod;\n")
                .append("\n")
                .append("\n")
                .append("public class %s implements EndpointClassInfo {\n")
                .append("\n")
                .append("    private static final Set<Operation> operations = ImmutableSet.<Operation>builder()\n")
                .append("            .add(\n")
                .append("                new Operation(RequestMethod.%s, %s)\n")
                .append("            )\n")
                .append("            .build();\n")
                .append("\n")
                .append("    @Override\n")
                .append("    public Set<Operation> operations() {\n")
                .append("        return operations;\n")
                .append("    }\n")
                .append("\n")
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
                .append("\n")
                .append("}\n")
                .toString();
        
        return String.format(
                expectedClassPattern, 
                typeInfo.className(),
                methodInfo.method(),
                methodInfo.path(),
                typeInfo.producedType().toLowerCase(),
                typeInfo.description(),
                typeInfo.rootPath()
        );
    }
}
