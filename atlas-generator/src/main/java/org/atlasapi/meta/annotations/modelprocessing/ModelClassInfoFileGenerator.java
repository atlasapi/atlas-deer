package org.atlasapi.meta.annotations.modelprocessing;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Writer;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;
import javax.tools.JavaFileObject;

import org.atlasapi.meta.annotations.FileGenerator;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class ModelClassInfoFileGenerator implements FileGenerator {

	private static final String OVERRIDDEN_METHOD_PATTERN = "%s@Override\n%spublic %%s %%s() {\n%sreturn %%s;\n%s}\n";
	private static final String SET_METHOD_ELEM_PATTERN = "%s.add(\n%sFieldInfo.builder()\n%s.withName(%%s)\n%s.withDescription(%%s)\n%s.withType(%%s)\n%s.build()\n%s)\n";
	private static final String CLASS_HEADER_PATTERN = "package %s;\n\n%s\n\npublic class %sInfo implements ModelClassInfo {\n";

	private static final String TAB = "    ";
	private static final String IMPORTS = "import java.util.Set;\n\nimport com.google.common.collect.ImmutableSet;\n\nimport org.atlasapi.meta.annotations.model.ModelClassInfo;\nimport org.atlasapi.meta.annotations.model.FieldInfo;";
	
	private final ImmutableList<Class<?>> classesToOutput;
	private final String overriddenMethodPattern;
	private final String setElemPattern;

	private ProcessingEnvironment processingEnv;
	
	public ModelClassInfoFileGenerator(Iterable<Class<?>> classesToOutput) {
		this.classesToOutput = ImmutableList.copyOf(classesToOutput);
		// TODO improve with format with repeated pattern
		this.overriddenMethodPattern = String.format(
				OVERRIDDEN_METHOD_PATTERN, 
				TAB, 
				TAB, 
				TAB + TAB, 
				TAB);
		this.setElemPattern = String.format(
				SET_METHOD_ELEM_PATTERN, 
				TAB + TAB + TAB, 
				TAB + TAB + TAB + TAB, 
				TAB + TAB + TAB + TAB + TAB,
				TAB + TAB + TAB + TAB + TAB,
				TAB + TAB + TAB + TAB + TAB,
				TAB + TAB + TAB + TAB + TAB,
				TAB + TAB + TAB);
	}
	
	@Override
    public void init(ProcessingEnvironment processingEnv) {
    	this.processingEnv = checkNotNull(processingEnv);
    }
	
	@Override
	public void processType(TypeElement type, Iterable<ExecutableElement> methods) {
		if (!shouldProcess(type)) {
			return;
		}

		PackageElement pkg = processingEnv.getElementUtils().getPackageOf(type);
		
		String classSource = generateSource(pkg, type, methods);
	    writeFile(pkg, type, classSource);
	}
	
	private String generateSource(PackageElement pkg, TypeElement type, Iterable<ExecutableElement> methods) {
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
		return TAB + "private static final Set<FieldInfo> fields = ImmutableSet.<FieldInfo>builder()\n";
	}
	
	private String setElementFrom(ExecutableElement method) {
		String fieldName = addQuotesToString(fieldNameFromAnnotation(method));
		String doc = addQuotesToString(parseJavadoc(processingEnv.getElementUtils().getDocComment(method)));
		String returnType = addQuotesToString(method.getReturnType().toString());
		return String.format(setElemPattern, fieldName, doc, returnType);
	}
	
	private String fieldNameFromAnnotation(ExecutableElement method) {
		FieldName annotation = method.getAnnotation(FieldName.class);
		return annotation.value();
	}

	private String setClose() {
		return TAB + TAB + TAB + ".build();\n";
	}
	
	private String createTypeMethods(TypeElement type) {
		StringBuilder methods = new StringBuilder();
		
		methods.append(formatOverriddenMethod("String", "name", addQuotesToString(typeToSimpleName(type).toLowerCase())));
		methods.append("\n");
		methods.append(formatOverriddenMethod("String", "description", addQuotesToString(parseJavadoc(processingEnv.getElementUtils().getDocComment(type)))));
		methods.append("\n");
		methods.append(formatOverriddenMethod("Set<FieldInfo>", "fields", "fields"));
		
		return methods.toString();
	}

	private String parseJavadoc(String docComment) {
		if (docComment == null) {
			return "";
		}
		return docComment.replace("\n", "");
	}

	private String addQuotesToString(String input) {
		return "\"" + input + "\"";
	}

	private String typeToSimpleName(TypeElement type) {
		return type.getSimpleName().toString();
	}
	
	private String formatOverriddenMethod(String returnType, String methodName, String returnValue) {
		return String.format(overriddenMethodPattern, returnType, methodName, returnValue);
	}

	private String classClose() {
		return "}\n";
	}

	private boolean shouldProcess(TypeElement type) {
		// TODO is this rigorous enough?
		return Iterables.contains(Iterables.transform(classesToOutput, new Function<Class<?>, String>() {
			@Override
			public String apply(Class<?> input) {
				return input.getCanonicalName();
			}
		}), type.getQualifiedName().toString());
	}

	private void writeFile(PackageElement pkg, TypeElement type, String typeSource) {
        processingEnv.getMessager().printMessage(Kind.NOTE, typeSource, type);
        String clsName = typeToSimpleName(type) + "Info";
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
