package org.atlasapi.generation;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import javax.annotation.processing.AbstractProcessor;

import org.atlasapi.generation.model.EndpointMethodInfo;
import org.atlasapi.generation.model.EndpointTypeInfo;
import org.atlasapi.generation.output.EndpointClassInfoSourceGenerator;
import org.atlasapi.generation.output.SourceGenerator;
import org.atlasapi.generation.output.writers.JavaxSourceFileWriter;
import org.atlasapi.generation.output.writers.SourceFileWriter;
import org.atlasapi.generation.parsing.EndpointTypeParser;
import org.atlasapi.generation.parsing.JavadocParser;
import org.atlasapi.generation.parsing.StandardJavadocParser;
import org.atlasapi.generation.parsing.TypeParser;
import org.atlasapi.generation.processing.ControllerAnnotationProcessor;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

// this tests both the processor and the wrapping generator, which isn't ideal
// it also writes out files into src/main/java, which is also suboptimal
// TODO remove the hardcoded filepaths
public class ProducesTypeProcessorTest {

    private static final List<Class<?>> sourceClasses = ImmutableList.<Class<?>>of(
            DummyController.class
    );
    
    private JavadocParser javadocParser = new StandardJavadocParser();
    private final MetaApiInfoClassGenerator generator = new MetaApiInfoClassGenerator();
    
    @Test
    public void testModelInfoClassGeneration() throws Exception {
        AbstractProcessor fieldNameProcessor = createFieldNameProcessor(sourceClasses);
        
        // TODO hacky - can this be avoided?
        MetaApiInfoClassGenerator.addPath("./src/test/java/");
        boolean hasFailed = generator.generateInfoClasses(fieldNameProcessor, sourceClasses);
        
        assertFalse("generation of model info classes failed", hasFailed);
        
        File file;
        for (Class<?> outputClass : sourceClasses) {
            file = new File(filePathFrom(outputClass));
            assertTrue("file " + outputClass.getSimpleName() + "EndpointInfo.java should exist but does not", file.exists());
        }
    }
    
    @After
    public void tearDown() {
        for (Class<?> outputClass : sourceClasses) {
            new File(filePathFrom(outputClass)).delete();
        }
    }

    private String filePathFrom(Class<?> outputClass) {
        return "./src/main/java/org/atlasapi/generation/generated/endpoints/" + outputClass.getSimpleName() + "EndpointInfo.java";
    }

    private AbstractProcessor createFieldNameProcessor(Iterable<Class<?>> classesToOutput) {
        SourceGenerator<EndpointTypeInfo, EndpointMethodInfo> classGenerator = new EndpointClassInfoSourceGenerator();
        SourceFileWriter<EndpointTypeInfo> writer = new JavaxSourceFileWriter<>();
        TypeParser<EndpointTypeInfo, EndpointMethodInfo> typeParser = new EndpointTypeParser(javadocParser);
        
        return new ControllerAnnotationProcessor(classGenerator, writer, typeParser, classesToOutput);
    }

}
