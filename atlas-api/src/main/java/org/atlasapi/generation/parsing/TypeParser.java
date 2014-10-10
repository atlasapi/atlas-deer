package org.atlasapi.generation.parsing;

import java.util.Set;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;

import org.atlasapi.generation.model.MethodInfo;
import org.atlasapi.generation.model.TypeInfo;

public interface TypeParser<TI extends TypeInfo, MI extends MethodInfo> {
	
	void init(ProcessingEnvironment processingEnv);
	TI parse(TypeElement type);
	Set<MI> parse(Iterable<ExecutableElement> methods);
}
