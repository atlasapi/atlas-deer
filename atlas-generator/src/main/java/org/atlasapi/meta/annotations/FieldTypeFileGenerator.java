package org.atlasapi.meta.annotations;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.util.Collection;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;
import javax.tools.JavaFileObject;

import org.apache.commons.lang3.text.WordUtils;

import com.google.common.base.Throwables;

public class FieldTypeFileGenerator implements FileGenerator {

	
	private final Class<? extends Annotation> type;

	private ProcessingEnvironment processingEnv;
	
	public FieldTypeFileGenerator(Class<? extends Annotation> type) {
		this.type = checkNotNull(type);
	}
	
	@Override
    public void init(ProcessingEnvironment processingEnv) {
    	this.processingEnv = checkNotNull(processingEnv);
    }
	
	@Override
	public void processType(TypeElement type, Collection<ExecutableElement> methods) {
		for (ExecutableElement method : methods) {
	        StringBuilder methodSource = new StringBuilder();
	            methodSource.append(methodAttribute(method));
	            writeFile(type, method, typeSource(type, method, methodSource.toString()));
	        }
	}
	
	private void writeFile(TypeElement type, ExecutableElement method, String typeSource) {
        processingEnv.getMessager().printMessage(Kind.NOTE, typeSource, type);
        String clsName = generatedName(method);
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

    private String typeSource(TypeElement type, ExecutableElement method, String methods) {
        PackageElement pkg = processingEnv.getElementUtils().getPackageOf(type);
        return String.format("package %s;\n\npublic final class %s implements Field {\n\n%s\n\n}\n", 
                pkg.getQualifiedName(), generatedSimpleName(method), methods);
    }

    private String generatedSimpleName(ExecutableElement method) {
    	FieldName fieldNameAnnotation = (FieldName) method.getAnnotation(type);
    	return capitalise(fieldNameAnnotation.value()) + "Field";
    }

    private String generatedName(ExecutableElement method) {
    	FieldName fieldNameAnnotation = (FieldName) method.getAnnotation(type);
    	return capitalise(fieldNameAnnotation.value()) + "Field"; 
    }

    // TODO templating?? ARE YOU MAD??!!
    private String methodAttribute(ExecutableElement method) {
    	FieldName fieldNameAnnotation = (FieldName) method.getAnnotation(type);
    	StringBuilder methods = new StringBuilder();
    	
    	methods.append(createMethod(fieldNameAnnotation.value(), "name"));
    	methods.append(createMethod(method.getReturnType().toString(), "value"));
        
        return methods.toString();
    }
    
    private String createMethod(String fieldValue, String apiField) {
    	return String.format(
    			"    @Override\n    public final String %s() {\n        return \"%s\";\n    }\n\n",
    			apiField,
    			fieldValue
		);
    }
    
    private String capitalise(String input) {
    	return WordUtils.capitalize(input);
    }
}
