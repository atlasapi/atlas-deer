package org.atlasapi.meta.annotations;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Map.Entry;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.Name;
import javax.tools.JavaFileObject;
import javax.tools.Diagnostic.Kind;

import org.atlasapi.meta.annotations.model.FieldInfo;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;

public class MetaInfoOutputter {

	private static final String PACKAGE_NAME = "org.atlasapi.meta.annotations";
	private static final String CLASS_NAME = "ParsedMetaAPIInfo";
	private final MetaApiInfoCollector collector;
	
	private ProcessingEnvironment processingEnv;
	
	public MetaInfoOutputter(MetaApiInfoCollector collector) {
		this.collector = checkNotNull(collector);
	}
	
	public void init(ProcessingEnvironment processingEnv) {
		this.processingEnv = processingEnv;
	}
	
	public void output() {
		String source = collateToJavaClassString();
		System.out.println(source);
		writeFile(source);
	}

	private void writeFile(String source) {
		String qualifiedName = Joiner.on('.').join(PACKAGE_NAME, CLASS_NAME);
        try {
			JavaFileObject srcFile = processingEnv.getFiler().createSourceFile(qualifiedName);
            Writer srcWriter = srcFile.openWriter();
            try {
                srcWriter.write(source);
            } finally {
                srcWriter.close();
            }
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(Kind.ERROR, "Failed to write " + qualifiedName
                    + ": " + Throwables.getStackTraceAsString(e));
        }
	}

	private String collateToJavaClassString() {
		StringBuilder classSource = new StringBuilder();
		
		createClassStart(classSource);
	 	
		for (Entry<String, Name> controllerType : collector.getTypeMapping().entrySet()) {
			Collection<FieldInfo> fieldsInfo = collector.getFieldMapping().get(controllerType.getValue());
			
			classSource.append("            .add(\n");
			// TODO fix var name
			classSource.append("                new Endpoint(\n");
			classSource.append(String.format("                    \"%s\",\n", controllerType.getKey()));
			classSource.append("                    \"TODO dummy description\",\n");
			classSource.append("                    ImmutableSet.<ValueField>builder()\n");
			for (FieldInfo fieldInfo : fieldsInfo) {
				// TODO actual values
				classSource.append(String.format(
						"                        .add(\n%s\n",
						createFieldValueFrom(fieldInfo, "                            ")
						));
				classSource.append("                        )\n");
			}
			classSource.append("                        .build()\n");
			classSource.append("                    )\n");
		}
	 	
	 	createClassEnd(classSource);
		
		return classSource.toString();
	}
	
	private String createFieldValueFrom(FieldInfo fieldInfo, String indent) {
		StringBuilder fieldInfoStr = new StringBuilder();
		
		fieldInfoStr.append(indent);
		fieldInfoStr.append("ValueField.builder()\n");
		fieldInfoStr.append(indent);
		fieldInfoStr.append("    .add(" + fieldInfo.name() + ")\n");
		fieldInfoStr.append(indent);
		fieldInfoStr.append("    .add(" + fieldInfo.description() + ")\n");
		fieldInfoStr.append(indent);
		fieldInfoStr.append("    .add(" + fieldInfo.type() + ")\n");
		Optional<String> endpoint = parseEndpointFrom(fieldInfo);
		if (endpoint.isPresent()) {
			fieldInfoStr.append(indent);
			fieldInfoStr.append("    .add(" + endpoint.get() + ")\n");
		}
		fieldInfoStr.append(indent);
		fieldInfoStr.append("    .build()");
		
		return fieldInfoStr.toString();
    }

	private Optional<String> parseEndpointFrom(FieldInfo fieldInfo) {
		// TODO this
		return Optional.absent();
	}

	private void createClassStart(StringBuilder classSource) {
		classSource.append(String.format("package %s;\n\npublic final class %s {\n\n", 
				"org.atlasapi.meta.annotations.model", "ParsedMetaAPIInfo"));
	 	classSource.append("    private final Map<String, Endpoint> endpoints = ImmutableMap.<String, Endpoint>builder()\n");
	}

	private void createClassEnd(StringBuilder classSource) {
		classSource.append("            .build();\n\n");
		classSource.append("    @Override\n");
		classSource.append("    public Set<Endpoint> allEndpoints() {\n");
		classSource.append("        return ImmutableSet.copyOf(endpoints.keyset());\n");
		classSource.append("    }\n\n");
		classSource.append("    @Override\n");
		classSource.append("    public Optional<Endpoint> endpointFor(String key) {\n");
		classSource.append("        return Optional.fromNullable(endpoints.get(key));\n");
		classSource.append("    }\n");
		classSource.append("}\n");
	}
}
