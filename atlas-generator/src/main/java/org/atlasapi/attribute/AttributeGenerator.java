package org.atlasapi.attribute;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic.Kind;

import com.google.auto.service.AutoService;
import com.google.common.base.Function;
import com.google.common.collect.Multimaps;

@AutoService(Processor.class)
public class AttributeGenerator extends AbstractProcessor {

    public AttributeGenerator() {
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(AtlasAttribute.class.getName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        boolean claimed = (annotations.size() == 1
            && annotations.iterator().next().getQualifiedName().toString().equals(
                AtlasAttribute.class.getName()));
        if (claimed) {
            process(roundEnv);
            return true;
        } else {
            return false;
        }
    }

    private void process(RoundEnvironment roundEnv) {
        Collection<? extends Element> annotatedElements =
            roundEnv.getElementsAnnotatedWith(AtlasAttribute.class);
        Collection<ExecutableElement> types = ElementFilter.methodsIn(annotatedElements);
        
        Multimaps.index(types, new Function<ExecutableElement, Element>() {
            @Override
            public Element apply(ExecutableElement input) {
                return input;
            }
        });
        
        for (ExecutableElement type : types) {
            try {
                processType(type);
            } catch (CompileException e) {
                // We abandoned this type, but continue with the next.
            } catch (RuntimeException e) {
                // Don't propagate this exception, which will confusingly crash
                // the compiler.
                processingEnv.getMessager().printMessage(Kind.ERROR, "@AtlasAttribute processor threw an exception: " + e, type);
            }
        }
    }

    private void processType(ExecutableElement type) throws CompileException {
        System.out.println(type.getEnclosingElement());
        System.out.println(type);
    }

    @SuppressWarnings("serial")
    private static class CompileException extends Exception {
    }
    
}
