package org.atlasapi.meta.annotations;

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;

import com.google.auto.service.AutoService;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;

@AutoService(Processor.class)
public class AnnotationProcessor extends AbstractProcessor {

	private static final Class<? extends Annotation> type = FieldName.class;
	
	private final FileGenerator generator;
	
	private Types typeUtils;

    public AnnotationProcessor(FileGenerator generator) {
		this.generator = checkNotNull(generator);
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(type.getName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        typeUtils = processingEnv.getTypeUtils();
        generator.init(processingEnv);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        boolean claimed = (annotations.size() == 1
            && annotations.iterator().next().getQualifiedName().toString().equals(
                type.getName()));
        if (claimed) {
            process(roundEnv);
            return true;
        } else {
            return false;
        }
    }

    private void process(RoundEnvironment roundEnv) {
        Collection<? extends Element> annotatedElements =
            roundEnv.getElementsAnnotatedWith(type);
        Collection<ExecutableElement> types = ElementFilter.methodsIn(annotatedElements);
        
        ImmutableListMultimap<TypeElement, ExecutableElement> typeMethod = Multimaps.index(types, 
            new Function<ExecutableElement, TypeElement>() {
                @Override
                public TypeElement apply(ExecutableElement input) {
                    return (TypeElement) input.getEnclosingElement();
                }
            }
        );
        
        typeMethod = collectSuperTypeMethods(typeMethod); 
        
        for (Entry<TypeElement, Collection<ExecutableElement>> typeMethods : typeMethod.asMap().entrySet()) {
            try {
            	generator.processType(typeMethods.getKey(), typeMethods.getValue());
            } catch (RuntimeException e) {
                processingEnv.getMessager().printMessage(Kind.ERROR, 
                        "@FieldName processor threw an exception: " + e, typeMethods.getKey());
            }
        }
    }

    private ImmutableListMultimap<TypeElement, ExecutableElement> collectSuperTypeMethods(
            ImmutableListMultimap<TypeElement, ExecutableElement> typeMethodIndex) {
        ImmutableListMultimap.Builder<TypeElement, ExecutableElement> builder
            = ImmutableListMultimap.builder();
        for (Entry<TypeElement, Collection<ExecutableElement>> typeAndMethods : typeMethodIndex.asMap().entrySet()) {
            builder.putAll(typeAndMethods.getKey(), typeAndMethods.getValue());
            for (TypeElement superType : superTypes(typeAndMethods.getKey())) {
                builder.putAll(typeAndMethods.getKey(), typeMethodIndex.get(superType));
            }
        }
        return builder.build();
    }

    private Iterable<TypeElement> superTypes(final TypeElement current) {
        return Lists.transform(typeUtils.directSupertypes(current.asType()),
            new Function<TypeMirror, TypeElement>(){
                @Override
                public TypeElement apply(TypeMirror input) {
                    return (TypeElement) processingEnv.getTypeUtils().asElement(input);
                }
            }
        );
    }
}
