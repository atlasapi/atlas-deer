package org.atlasapi.generation.model;

import static com.google.common.base.Preconditions.checkNotNull;

import org.elasticsearch.common.base.Objects;

public class ModelTypeInfo implements TypeInfo {
    
    private static final String MODEL_PACKAGE = "org.atlasapi.generation.generated.model";
	
    private final String key;
	private final String outputClassName;
	private final String parsedClass;
	private final String description;
	
	public static Builder builder() {
		return new Builder();
	}

	private ModelTypeInfo(String key, String outputClassName, String parsedClass, 
	        String description) {
	    this.key = checkNotNull(key);
        this.outputClassName = checkNotNull(outputClassName);
        this.parsedClass = checkNotNull(parsedClass);
		this.description = checkNotNull(description);
	}
	
	public String key() {
	    return key;
	}
	
	@Override
	public String className() {
	    return outputClassName;
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
		        .add("key", key)
				.add("outputClassName", outputClassName)
				.add("package", MODEL_PACKAGE)
				.add("parsedClass", parsedClass)
				.add("description", description)
				.toString();
	}
	
	public static class Builder {
		
	    private String key;
		private String outputClassName;
		private String description;
		private String parsedClass;

		private Builder() {}
		
		public ModelTypeInfo build() {
			return new ModelTypeInfo(key, outputClassName, parsedClass, description);
		}
        
        public Builder withKey(String key) {
            this.key = key;
            return this;
        }
		
		public Builder withOutputClassName(String outputClassName) {
		    this.outputClassName = outputClassName;
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
