package org.atlasapi.attribute;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.springframework.core.io.ClassPathResource;
import org.springframework.util.ClassUtils;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

public class AttributeGeneratorTest {

    private static final String JAVA = ".java";

    @Test
    public void testGeneratingAttributes() throws Exception {
        
        ImmutableList<Class<?>> classes = ImmutableList.<Class<?>>of(
            ParentTestClass.class, 
            TestClass.class
        );
        ImmutableSet<AttributeGenerator> processors = ImmutableSet.of(new AttributeGenerator());
        
        List<Diagnostic<? extends JavaFileObject>> diagnostics = compileWithProcessors(classes, processors);
        
        for (Diagnostic<?> diagnostic : diagnostics) {
            System.out.println(diagnostic.getMessage(Locale.getDefault()));
        }
        
    }

    private List<Diagnostic<? extends JavaFileObject>> compileWithProcessors(
            ImmutableList<Class<?>> classes, ImmutableSet<AttributeGenerator> processors) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        
        DiagnosticCollector<JavaFileObject> diagnosticCollector = 
                new DiagnosticCollector<JavaFileObject>();
        StandardJavaFileManager fileManager = 
                compiler.getStandardFileManager(diagnosticCollector, null, null);
        
        List<File> compilationFiles = Lists.transform(classes, new Function<Class<?>, File>() {
            @Override
            public File apply(Class<?> input) {
                String path = input.getName().replace(".", "/") + JAVA;
                try {
                    return new File(Resources.getResource(path).toURI());
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        
        Iterable<? extends JavaFileObject> compilationUnits
            = fileManager.getJavaFileObjectsFromFiles(compilationFiles);
        
        CompilationTask task = compiler.getTask(new OutputStreamWriter(System.out), 
                fileManager, diagnosticCollector, 
                Arrays.asList("-proc:only"), null, compilationUnits);
        
        task.setProcessors(processors);
        task.call();
        
        try {
            fileManager.close();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        return diagnosticCollector.getDiagnostics();
    }
}
