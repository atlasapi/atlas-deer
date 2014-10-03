package org.atlasapi.generation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Writer;
import java.util.Set;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;
import javax.tools.JavaFileObject;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

public class EndpointClassInfoFileGenerator implements FileGenerator {
	
	private static final String OVERRIDDEN_METHOD_PATTERN = "%s@Override\n%spublic %%s %%s() {\n%sreturn %%s;\n%s}\n";
	private static final String SET_METHOD_ELEM_PATTERN = "%s.add(new Operation(%%s, %%s))\n";
	private static final String CLASS_HEADER_PATTERN = "package %s;\n\n%s\n\npublic class %sEndpointInfo implements EndpointClassInfo {\n";
	
	private static final String TAB = "    ";
	private static final String IMPORTS = "import java.util.Set;\n\nimport com.google.common.collect.ImmutableSet;\n\nimport org.atlasapi.meta.annotations.model.EndpointClassInfo;\nimport org.atlasapi.meta.annotations.model.Operation;\n\nimport org.springframework.web.bind.annotation.RequestMethod;";
	
	private final String overriddenMethodPattern;
	private final String setElemPattern;
	
	private ProcessingEnvironment processingEnv;
	
	public EndpointClassInfoFileGenerator() {
		this.overriddenMethodPattern = String.format(
				OVERRIDDEN_METHOD_PATTERN, 
				TAB, 
				TAB, 
				TAB + TAB, 
				TAB);
		this.setElemPattern = String.format(
				SET_METHOD_ELEM_PATTERN, 
				TAB + TAB + TAB
		);
	}

	@Override
	public void init(ProcessingEnvironment processingEnv) {
		this.processingEnv = checkNotNull(processingEnv);
	}

	@Override
	public void processType(TypeElement type,
			Iterable<ExecutableElement> methods) {
		
		PackageElement pkg = processingEnv.getElementUtils().getPackageOf(type);
		
		String classSource = generateSource(pkg, type, methods);
	    writeFile(pkg, type, classSource);
	}

	private String generateSource(PackageElement pkg, TypeElement type,
			Iterable<ExecutableElement> methods) {
		StringBuilder source = new StringBuilder();
		
		source.append(formatClassHeader(pkg, type));
		source.append("\n");
		source.append(setInstantiation());
		for (ExecutableElement method : methods) {
			source.append(setElementFrom(method));
		}
		source.append(setClose());
		source.append("\n");
		source.append(createTypeMethods(type));
		source.append(classClose());
		
		return source.toString();
	}
	
	private String formatClassHeader(PackageElement pkg, TypeElement type) {
		return String.format(CLASS_HEADER_PATTERN, pkg.getQualifiedName().toString(), createImports(), typeToSimpleName(type));
	}

	private String createImports() {
		return IMPORTS;
	}

	private String setInstantiation() {
		return TAB + "private static final Set<Operation> operations = ImmutableSet.<Operation>builder()\n";
	}
	
	private String setElementFrom(ExecutableElement method) {
		RequestMapping requestMappingAnnotation = method.getAnnotation(RequestMapping.class);
		Set<RequestMethod> methods = extractMethods(requestMappingAnnotation);
		Set<String> paths = extractPaths(requestMappingAnnotation);
		
		StringBuilder operations = new StringBuilder();
		for (RequestMethod requestMethod : methods) {
			for (String path : paths) {
				operations.append(String.format(setElemPattern, addQuotesToString(path), requestMethod));
			}
		}
		return operations.toString();
	}

	private Set<RequestMethod> extractMethods(RequestMapping requestMapping) {
		RequestMethod[] requestMethods = requestMapping.method();
		if (requestMethods.length == 0) {
			return ImmutableSet.of(RequestMethod.GET);
		}
		return ImmutableSet.copyOf(requestMethods);
	}
	
	private Set<String> extractPaths(RequestMapping requestMapping) {
		return ImmutableSet.copyOf(requestMapping.value());
	}

	private String setClose() {
		return TAB + TAB + TAB + ".build();\n";
	}
	
	private String createTypeMethods(TypeElement type) {
		StringBuilder methods = new StringBuilder();
		
		methods.append(formatOverriddenMethod("String", "name", addQuotesToString(typeToSimpleName(type))));
		methods.append("\n");
		methods.append(formatOverriddenMethod("String", "description", addQuotesToString(parseJavadoc(processingEnv.getElementUtils().getDocComment(type)))));
		methods.append("\n");
		methods.append(formatOverriddenMethod("String", "rootPath", addQuotesToString(extractRootPath(type).or(""))));
		methods.append("\n");
		methods.append(formatOverriddenMethod("Set<Operation>", "operations", "operations"));
		methods.append("\n");
		methods.append(formatOverriddenMethod("Class<?>", "returnedType", extractProducedType(type) + ".class"));
		
		return methods.toString();
	}

	private Optional<String> extractRootPath(TypeElement type) {
		RequestMapping rootMapping = type.getAnnotation(RequestMapping.class);
		if (rootMapping == null) {
			return Optional.absent();
		}
		// TODO potential issue if multiple root paths specified
		return Optional.of(rootMapping.value()[0]);
	}

	private String formatOverriddenMethod(String returnType, String methodName, String returnValue) {
		return String.format(overriddenMethodPattern, returnType, methodName, returnValue);
	}

	private String extractProducedType(TypeElement type) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private String addQuotesToString(String input) {
		return "\"" + input + "\"";
	}

	private String parseJavadoc(String docComment) {
		if (docComment == null) {
			return "";
		}
		return docComment.replace("\n", "");
	}

	private String classClose() {
		return "}\n";
	}

	private String typeToSimpleName(TypeElement type) {
		return type.getSimpleName().toString();
	}

	private void writeFile(PackageElement pkg, TypeElement type,
			String typeSource) {
		processingEnv.getMessager().printMessage(Kind.NOTE, typeSource, type);
        String clsName = typeToSimpleName(type) + "EndpointInfo";
        try {
            String fullName = Joiner.on('.').join(pkg.getQualifiedName().toString(), clsName);
			JavaFileObject srcFile = processingEnv.getFiler().createSourceFile(fullName, type);
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

}
