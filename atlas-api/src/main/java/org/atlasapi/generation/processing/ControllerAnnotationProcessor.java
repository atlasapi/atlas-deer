package org.atlasapi.generation.processing;

import java.lang.annotation.Annotation;
import java.util.Set;

import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.ElementFilter;

import org.atlasapi.generation.model.EndpointMethodInfo;
import org.atlasapi.generation.model.EndpointTypeInfo;
import org.atlasapi.generation.output.SourceGenerator;
import org.atlasapi.generation.output.writers.SourceFileWriter;
import org.atlasapi.generation.parsing.TypeParser;
import org.atlasapi.meta.annotations.ProducesType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public final class ControllerAnnotationProcessor extends SingleAnnotationTypeProcessor<EndpointTypeInfo, EndpointMethodInfo> {

    public ControllerAnnotationProcessor(SourceGenerator<EndpointTypeInfo, EndpointMethodInfo> generator, 
            SourceFileWriter<EndpointTypeInfo> writer, TypeParser<EndpointTypeInfo, EndpointMethodInfo> typeParser, 
            Iterable<Class<?>> classesToOutput) {
        super(ProducesType.class, classesToOutput, typeParser, generator, writer);
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
    }

    @Override
    public void process(RoundEnvironment roundEnv) {
    	Set<TypeElement> annotatedTypes = ElementFilter.typesIn(roundEnv.getElementsAnnotatedWith(annotationType()));
        Iterable<TypeElement> controllerTypes = filterByAnnotation(annotatedTypes, Controller.class);
    	
    	for (TypeElement controller : controllerTypes) {
    		Iterable<ExecutableElement> requestMethods = filterByAnnotation(
    		        ElementFilter.methodsIn(controller.getEnclosedElements()), 
    		        RequestMapping.class
	        );
    		processTypeAndMethods(controller, requestMethods);
    	}
    }

    private <E extends Element> Iterable<E> filterByAnnotation(Iterable<E> elements, 
            final Class<? extends Annotation> annotationType) {
        return Iterables.filter(elements, new Predicate<E>() {
        	@Override
        	public boolean apply(E input) {
        		return input.getAnnotation(annotationType) != null;
        	}
        });
    }
}
