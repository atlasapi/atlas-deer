package org.atlasapi.generation.model;

import static com.google.common.base.Preconditions.checkNotNull;

import org.elasticsearch.common.base.Objects;

public class ModelTypeInfo implements TypeInfo {
    
    private static final String MODEL_PACKAGE = "org.atlasapi.generation.generated.model";
	
	private final String typeName;
	private final String className;
	private final String parsedClass;
	private final String description;
	
	public static Builder builder() {
		return new Builder();
	}

	private ModelTypeInfo(String typeName, String className, 
	        String parsedClass, String description) {
        this.typeName = checkNotNull(typeName);
        this.className = checkNotNull(className);
        this.parsedClass = checkNotNull(parsedClass);
		this.description = checkNotNull(description);
	}

	public String typeName() {
		return typeName;
	}
	
	@Override
	public String className() {
	    return className;
	}

	@Override
	public String fullPackage() {
	    return MODEL_PACKAGE;
	}
	
	public String description() {
		return description;
	}
	
	public String parsedClass() {
	    return parsedClass;
	}
	
	@Override
	public String toString() {
		return Objects.toStringHelper(getClass())
				.add("typeName", typeName)
				.add("className", className)
				.add("package", MODEL_PACKAGE)
				.add("parsedClass", parsedClass)
				.add("description", description)
				.toString();
	}
	
	public static class Builder {
		
		private String typeName;
		private String className;
		private String description;
		private String parsedClass;

		private Builder() {}
		
		public ModelTypeInfo build() {
			return new ModelTypeInfo(typeName, className, parsedClass, description);
		}
		
		public Builder withTypeName(String typeName) {
			this.typeName = typeName;
			return this;
		}
		
		public Builder withClassName(String className) {
		    this.className = className;
		    return this;
		}
		
		public Builder withParsedClass(String parsedClass) {
		    this.parsedClass = parsedClass;
		    return this;
		}

		public Builder withDescription(String description) {
			this.description = description;
			return this;
		}
	}
}
