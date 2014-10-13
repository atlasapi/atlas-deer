package org.atlasapi.generation;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import javax.annotation.processing.AbstractProcessor;

import org.atlasapi.generation.model.ModelMethodInfo;
import org.atlasapi.generation.model.ModelTypeInfo;
import org.atlasapi.generation.output.ModelClassInfoSourceGenerator;
import org.atlasapi.generation.output.SourceGenerator;
import org.atlasapi.generation.output.writers.JavaxSourceFileWriter;
import org.atlasapi.generation.output.writers.SourceFileWriter;
import org.atlasapi.generation.parsing.JavadocParser;
import org.atlasapi.generation.parsing.ModelTypeParser;
import org.atlasapi.generation.parsing.StandardJavadocParser;
import org.atlasapi.generation.parsing.TypeParser;
import org.atlasapi.generation.processing.FieldNameProcessor;
import org.junit.Test;

import com.google.common.collect.ImmutableList;


public class MetaApiInfoClassGeneratorTest {

    private JavadocParser javadocParser = new StandardJavadocParser();
    private final MetaApiInfoClassGenerator generator = new MetaApiInfoClassGenerator();
    
    @Test
    public void testModelInfoClassGeneration() throws Exception {
        List<Class<?>> sourceClasses = ImmutableList.<Class<?>>of(
                TopLevelClass.class,
                MidLevelClass.class,
                BottomLevelClass.class
        );
        ImmutableList<Class<?>> outputClasses = ImmutableList.<Class<?>>of(
                MidLevelClass.class, 
                BottomLevelClass.class
        );
        
        AbstractProcessor fieldNameProcessor = createFieldNameProcessor(outputClasses);
        
        // hacking in the test
        MetaApiInfoClassGenerator.addPath("/Users/oli/Documents/Code/atlas-deer/atlas-api/src/test/java/");
        boolean hasFailed = generator.generateInfoClasses(fieldNameProcessor, sourceClasses);
        
        assertFalse("generation of model info classes failed", hasFailed);
        
        File file;
        for (Class<?> outputClass : outputClasses) {
            file = new File("/Users/oli/Documents/Code/atlas-deer/atlas-api/src/main/java/org/atlasapi/generation/generated/model/" + outputClass.getSimpleName() + "Info.java");
            assertTrue("file " + outputClass.getSimpleName() + "Info.java should exist but does not", file.exists());
            // knarly - does clean-up in test 
            file.delete();
        }
    }

    private AbstractProcessor createFieldNameProcessor(Iterable<Class<?>> classesToOutput) {
        SourceGenerator<ModelTypeInfo, ModelMethodInfo> classGenerator = new ModelClassInfoSourceGenerator();
        HierarchyExtractor hierarchyExtractor = new ReflectionBasedHierarchyExtractor();
        SourceFileWriter<ModelTypeInfo> writer = new JavaxSourceFileWriter<>();
        TypeParser<ModelTypeInfo, ModelMethodInfo> typeParser = new ModelTypeParser(javadocParser, classesToOutput);
        return new FieldNameProcessor(classGenerator, hierarchyExtractor, writer, typeParser, classesToOutput);
    }

}
