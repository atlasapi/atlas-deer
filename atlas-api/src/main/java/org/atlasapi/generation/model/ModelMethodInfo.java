package org.atlasapi.generation.model;

import com.google.common.base.MoreObjects;

import static com.google.common.base.Preconditions.checkNotNull;

public class ModelMethodInfo implements MethodInfo {

    private final String name;
    private final String description;
    private final String type;
    private final JsonType jsonType;
    private final Boolean isMultiple;
    private final Boolean isModelType;

    public static Builder builder() {
        return new Builder();
    }

    private ModelMethodInfo(String name, String description, String type,
            JsonType jsonType, Boolean isMultiple, Boolean isModelType) {
        this.name = checkNotNull(name);
        this.description = checkNotNull(description);
        this.type = checkNotNull(type);
        this.jsonType = checkNotNull(jsonType);
        this.isMultiple = checkNotNull(isMultiple);
        this.isModelType = checkNotNull(isModelType);
    }

    public String name() {
        return name;
    }

    public String description() {
        return description;
    }

    public String type() {
        return type;
    }

    public JsonType jsonType() {
        return jsonType;
    }

    public Boolean isMultiple() {
        return isMultiple;
    }

    public Boolean isModelType() {
        return isModelType;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("name", name)
                .add("description", description)
                .add("type", type)
                .add("jsonType", jsonType)
                .add("isMultiple", isMultiple)
                .add("isModelType", isModelType)
                .toString();
    }

    public static class Builder {

        private String name;
        private String description;
        private String type;
        private JsonType jsonType;
        private Boolean isMultiple;
        private Boolean isModelType;

        private Builder() {
        }

        public ModelMethodInfo build() {
            return new ModelMethodInfo(name, description, type, jsonType, isMultiple,
                    isModelType
            );
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public Builder withJsonType(JsonType jsonType) {
            this.jsonType = jsonType;
            return this;
        }

        public Builder withIsMultiple(Boolean isMultiple) {
            this.isMultiple = isMultiple;
            return this;
        }

        public Builder withIsModelType(Boolean isModelType) {
            this.isModelType = isModelType;
            return this;
        }
    }
}
