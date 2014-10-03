package org.atlasapi.meta.annotations;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Writer;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.JavaFileObject;
import javax.tools.Diagnostic.Kind;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

public class EndpointClassInfoFileGenerator implements FileGenerator {
	
	private static final String OVERRIDDEN_METHOD_PATTERN = "%s@Override\n%spublic %%s %%s() {\n%sreturn %%s;\n%s}\n";
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
		// TODO
		this.setElemPattern = "";
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
//		String fieldName = addQuotesToString(fieldNameFromAnnotation(method));
//		String doc = addQuotesToString(parseJavadoc(processingEnv.getElementUtils().getDocComment(method)));
//		String returnType = addQuotesToString(method.getReturnType().toString());
//		return String.format(setElemPattern, fieldName, doc, returnType);
		// TODO
		return "";
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
		// TODO root path
		methods.append("\n");
		methods.append(formatOverriddenMethod("Set<Operation>", "operations", "operations"));
		methods.append("\n");
		// TODO returned type
		
		return methods.toString();
	}
	
	private String formatOverriddenMethod(String returnType, String methodName, String returnValue) {
		return String.format(overriddenMethodPattern, returnType, methodName, returnValue);
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
