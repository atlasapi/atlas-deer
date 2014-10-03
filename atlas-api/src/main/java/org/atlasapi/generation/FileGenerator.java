package org.atlasapi.generation;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;

public interface FileGenerator {
	
	void init(ProcessingEnvironment processingEnv);
	void processType(TypeElement type, Iterable<ExecutableElement> methods);
}
