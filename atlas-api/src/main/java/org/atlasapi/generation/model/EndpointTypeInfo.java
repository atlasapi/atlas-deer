package org.atlasapi.generation.model;

import static com.google.common.base.Preconditions.checkNotNull;

import org.elasticsearch.common.base.Objects;


public class EndpointTypeInfo implements TypeInfo {
    
    private static final String ENDPOINT_PACKAGE = "org.atlasapi.generation.generated.endpoints";

    private final String typeName;
    private final String className;
    private final String description;
    private final String rootPath;
    private final String producedType;
    
    public static Builder builder() {
        return new Builder();
    }

    private EndpointTypeInfo(String typeName, String className,
            String description, String rootPath, String producedType) {
        this.typeName = checkNotNull(typeName);
        this.className = checkNotNull(className);
        this.description = checkNotNull(description);
        this.rootPath = checkNotNull(rootPath);
        this.producedType = checkNotNull(producedType);
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
        return ENDPOINT_PACKAGE;
    }

    public String description() {
        return description;
    }
    
    public String rootPath() {
        return rootPath;
    }
    
    public String producedType() {
        return producedType;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("typeName", typeName)
                .add("className", className)
                .add("package", ENDPOINT_PACKAGE)
                .add("description", description)
                .add("rootPath", rootPath)
                .add("producedType", producedType)
                .toString();
    }
    
    public static class Builder {
        
        private String typeName;
        private String className;
        private String description;
        private String rootPath;
        private String producedType;

        private Builder() {}
        
        public EndpointTypeInfo build() {
            return new EndpointTypeInfo(typeName, className, description, rootPath, producedType);
        }
        
        public Builder withTypeName(String typeName) {
            this.typeName = typeName;
            return this;
        }
        
        public Builder withClassName(String className) {
            this.className = className;
            return this;
        }
        
        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withRootPath(String rootPath) {
            this.rootPath = rootPath;
            return this;
        }

        public Builder withProducedType(String producedType) {
            this.producedType = producedType;
            return this;
        }
    }
}
