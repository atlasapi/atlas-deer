package org.atlasapi.generation.processing;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.ElementFilter;

import org.atlasapi.generation.HierarchyExtractor;
import org.atlasapi.generation.model.ModelMethodInfo;
import org.atlasapi.generation.model.ModelTypeInfo;
import org.atlasapi.generation.output.SourceGenerator;
import org.atlasapi.generation.output.writers.SourceFileWriter;
import org.atlasapi.generation.parsing.TypeParser;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimaps;

import static com.google.common.base.Preconditions.checkNotNull;

public final class FieldNameProcessor
        extends SingleAnnotationTypeProcessor<ModelTypeInfo, ModelMethodInfo> {

    private final HierarchyExtractor hierarchyExtractor;

    public FieldNameProcessor(SourceGenerator<ModelTypeInfo, ModelMethodInfo> generator,
            HierarchyExtractor hierarchyExtractor, SourceFileWriter<ModelTypeInfo> writer,
            TypeParser<ModelTypeInfo, ModelMethodInfo> typeParser,
            Iterable<Class<?>> classesToOutput) {
        super(FieldName.class, classesToOutput, typeParser, generator, writer);
        this.hierarchyExtractor = checkNotNull(hierarchyExtractor);
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        hierarchyExtractor.init(processingEnv.getTypeUtils());
    }

    @Override
    public void process(RoundEnvironment roundEnv) {
        Collection<ExecutableElement> types = annotatedMethods(roundEnv);

        ImmutableListMultimap<TypeElement, ExecutableElement> typeMethodMapping = Multimaps.index(
                types,
                new Function<ExecutableElement, TypeElement>() {

                    @Override
                    public TypeElement apply(ExecutableElement input) {
                        return (TypeElement) input.getEnclosingElement();
                    }
                }
        );

        typeMethodMapping = collectSuperTypeMethods(typeMethodMapping);
        for (Entry<TypeElement, Collection<ExecutableElement>> typeMethods : typeMethodMapping.asMap()
                .entrySet()) {
            processTypeAndMethods(typeMethods.getKey(), typeMethods.getValue());
        }
    }

    private Collection<ExecutableElement> annotatedMethods(RoundEnvironment roundEnv) {
        Collection<? extends Element> annotatedElements = roundEnv.getElementsAnnotatedWith(
                annotationType());
        return ElementFilter.methodsIn(annotatedElements);
    }

    private ImmutableListMultimap<TypeElement, ExecutableElement> collectSuperTypeMethods(
            ImmutableListMultimap<TypeElement, ExecutableElement> typeMethodIndex) {
        ImmutableListMultimap.Builder<TypeElement, ExecutableElement> builder = ImmutableListMultimap
                .builder();
        for (Entry<TypeElement, Collection<ExecutableElement>> typeAndMethods : typeMethodIndex.asMap()
                .entrySet()) {
            builder.putAll(typeAndMethods.getKey(), typeAndMethods.getValue());
            Set<TypeElement> fullUpwardHierarchy = hierarchyExtractor.fullHierarchy(typeAndMethods.getKey());
            for (TypeElement superType : fullUpwardHierarchy) {
                builder.putAll(typeAndMethods.getKey(), typeMethodIndex.get(superType));
            }
        }
        return builder.build();
    }
}
