package org.atlasapi.attribute;

import java.io.IOException;
import java.io.Writer;
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
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;
import javax.tools.JavaFileObject;

import com.google.auto.service.AutoService;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;

@AutoService(Processor.class)
public class AttributeGenerator extends AbstractProcessor {

    private Types typeUtils;
    private Elements elemUtils;

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
        typeUtils = processingEnv.getTypeUtils();
        elemUtils = processingEnv.getElementUtils();
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
                processType(typeMethods.getKey(), typeMethods.getValue());
            } catch (RuntimeException e) {
                processingEnv.getMessager().printMessage(Kind.ERROR, 
                        "@AtlasAttribute processor threw an exception: " + e, typeMethods.getKey());
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

    private void processType(TypeElement type, Collection<ExecutableElement> methods) {
        StringBuilder methodSource = new StringBuilder();
        for (ExecutableElement method : methods) {
            methodSource.append(methodAttribute(method));
        }
        writeFile(type, typeSource(type, methodSource.toString()));
    }
    
    private void writeFile(TypeElement type, String typeSource) {
        processingEnv.getMessager().printMessage(Kind.NOTE, typeSource, type);
        String clsName = generatedName(type);
        try {
            JavaFileObject srcFile = processingEnv.getFiler().createSourceFile(clsName, type);
            Writer srcWriter = srcFile.openWriter();
            try {
                srcWriter.write(typeSource);
            } finally {
                srcWriter.close();
            }
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(Kind.ERROR, "Failed to write " + clsName 
                    + ": " + Throwables.getStackTraceAsString(e));
        }
    }

    private String typeSource(TypeElement type, String methods) {
        PackageElement pkg = elemUtils.getPackageOf(type);
        return String.format("package %s;\n\n public final class %s {\n\n%s\n\n}\n", 
                pkg.getQualifiedName(), generatedSimpleName(type), methods);
    }

    private String generatedSimpleName(TypeElement type) {
        return type.getSimpleName()+"Attributes";
    }

    private String generatedName(TypeElement type) {
        return type.getQualifiedName()+"Attributes";
    }

    private String methodAttribute(ExecutableElement method) {
        return String.format("private final String %s = \"%s\";\n", method.getSimpleName(),method.getSimpleName());
    }
    
}
