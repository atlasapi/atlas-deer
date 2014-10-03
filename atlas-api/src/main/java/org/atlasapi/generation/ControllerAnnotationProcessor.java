package org.atlasapi.generation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic.Kind;

import org.atlasapi.meta.annotations.ProducesType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.google.auto.service.AutoService;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

@AutoService(Processor.class)
public class ControllerAnnotationProcessor extends AbstractProcessor {

	private static final Class<? extends Annotation> controllerModelAnnotation = ProducesType.class;
	
	private final FileGenerator generator;
	
	public ControllerAnnotationProcessor(FileGenerator generator) {
		this.generator = checkNotNull(generator);
	}
	
    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(controllerModelAnnotation.getName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        generator.init(processingEnv);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        boolean claimed = (annotations.size() == 1
            && annotations.iterator().next().getQualifiedName().toString().equals(
                controllerModelAnnotation.getName()));
        if (claimed) {
            process(roundEnv);
            return true;
        } else {
            return false;
        }
    }

    private void process(RoundEnvironment roundEnv) {
    	// get types annotated with both producesType and Controller annotation
    	Set<? extends Element> potentialControllers = roundEnv.getElementsAnnotatedWith(controllerModelAnnotation);
    	Set<TypeElement> potentialControllerTypes = ElementFilter.typesIn(potentialControllers);
    	Iterable<TypeElement> controllerTypes = Iterables.filter(potentialControllerTypes, new Predicate<TypeElement>() {
			@Override
			public boolean apply(TypeElement input) {
//				return input.getAnnotationMirrors().contains(Controller.class);
				return input.getAnnotation(Controller.class) != null;
			}
		});
    	
    	// get methods on those types that have RequestMapping annotation
    	for (TypeElement controller : controllerTypes) {
    		List<ExecutableElement> controllerMethods = ElementFilter.methodsIn(controller.getEnclosedElements());
    		// process those in the generator
    		Iterable<ExecutableElement> requestMethods = Iterables.filter(controllerMethods, new Predicate<ExecutableElement>() {
    			@Override
    			public boolean apply(ExecutableElement input) {
//    				return input.getAnnotationMirrors().contains(RequestMapping.class);
    				return input.getAnnotation(RequestMapping.class) != null;
    			}
    		});
    		try {
    			generator.processType(controller, requestMethods);
    		} catch (RuntimeException e) {
    			processingEnv.getMessager().printMessage(Kind.ERROR, 
    					"Controller processor threw an exception: " + e, controller);
    		}
    	}
    }

	private String parseKey(TypeElement controllerType) {
		RequestMapping requestMapping = controllerType.getAnnotation(RequestMapping.class);
		// TODO fragile, will fail if duplicate root paths
		return requestMapping.value()[0];
	}

	private TypeElement extractControllerType(TypeElement controllerClass) {
		try {
			ProducesType annotation = controllerClass.getAnnotation(ProducesType.class);
			// catch case where controller not annotated with this annotation
			// should also check that this is ACTUALLY a controller, just for safety and general peace of mind
			Class<?> typeyType = annotation.type();
			throw new RuntimeException("Should never be reached - annotation.type() should throw");
		} catch (MirroredTypeException e) {
			TypeMirror typeMirror = e.getTypeMirror();
			DeclaredType dt = (DeclaredType) typeMirror;
			TypeElement typeElement = (TypeElement) dt.asElement();
			return typeElement;
        }
	}
}
