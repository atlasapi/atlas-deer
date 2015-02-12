package org.atlasapi.generation.processing;

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;

import org.atlasapi.generation.model.MethodInfo;
import org.atlasapi.generation.model.TypeInfo;
import org.atlasapi.generation.output.SourceGenerator;
import org.atlasapi.generation.output.writers.SourceFileWriter;
import org.atlasapi.generation.parsing.TypeParser;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public abstract class SingleAnnotationTypeProcessor<TI extends TypeInfo, MI extends MethodInfo> extends AbstractProcessor {
	
	private final Class<? extends Annotation> annotationType;
	private final ImmutableList<Class<?>> classesToOutput;
	private final TypeParser<TI, MI> typeParser;
    private final SourceGenerator<TI, MI> generator;
    private final SourceFileWriter<TI> writer;
	
	public SingleAnnotationTypeProcessor(Class<? extends Annotation> annotationType,
    		Iterable<Class<?>> classesToOutput, TypeParser<TI, MI> typeParser, 
    		SourceGenerator<TI, MI> generator, SourceFileWriter<TI> writer) {
		this.annotationType = checkNotNull(annotationType);
		this.classesToOutput = ImmutableList.copyOf(classesToOutput);
        this.typeParser = checkNotNull(typeParser);
        this.generator = checkNotNull(generator);
        this.writer = checkNotNull(writer);
	}
	
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        writer.init(processingEnv.getFiler(), processingEnv.getMessager());
        typeParser.init(processingEnv);
    }

    @Override
    public final SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public final Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(annotationType.getName());
    }
    
    public Class<? extends Annotation> annotationType() {
    	return annotationType;
    }

    @Override
    public final boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (isClaimed(annotations)) {
            process(roundEnv);
            return true;
        } else {
            return false;
        }
    }

    // TODO does this miss explicitly overridden methods?
	private boolean isClaimed(Set<? extends TypeElement> annotations) {
		return annotations.size() == 1
            && annotations.iterator().next().getQualifiedName().toString().equals(annotationType().getName());
	}
	
	protected boolean shouldProcess(TypeElement type) {
		return Iterables.contains(Iterables.transform(classesToOutput, new Function<Class<?>, String>() {
			@Override
			public String apply(Class<?> input) {
				return input.getCanonicalName();
			}
		}), type.getQualifiedName().toString());
	}
    
    public abstract void process(RoundEnvironment roundEnv);
    
    protected void processTypeAndMethods(TypeElement type, Iterable<ExecutableElement> methods) {
        try {
            if (!shouldProcess(type)) {
                processingEnv.getMessager().printMessage(Kind.NOTE, "No source generated for " + type.toString());
                return;
            }

            TI typeInfo = typeParser.parse(type);
            String generatedSource = generator.processType(typeInfo, typeParser.parse(methods));
            writer.writeFile(typeInfo, generatedSource);
            
        } catch (RuntimeException e) {
            processingEnv.getMessager().printMessage(
                    Kind.ERROR,
                    String.format(
                            "@FieldName processor threw an exception: %s while processing element %s",
                            e,
                            type
                    )
                    , type);
        }
    }
}
