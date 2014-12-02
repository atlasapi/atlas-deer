package org.atlasapi.generation.model;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;

public class FieldInfo {
	
	private final String name;
	private final String description;
	private final String type;
	private final JsonType jsonType;
	private final Boolean isMultiple;
	private final Boolean isModelType;
	
	public static Builder builder() {
		return new Builder();
	}
	
	private FieldInfo(String name, String description, String type, JsonType jsonType,
	        Boolean isMultiple, Boolean isModelType) {
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
	public int hashCode() {
		return Objects.hashCode(name);
	}
	
	@Override
	public String toString() {
		return Objects.toStringHelper(FieldInfo.class)
				.add("name", name)
				.add("description", description)
				.add("type", type)
				.add("jsonType", jsonType)
				.add("isMultiple", isMultiple)
                .add("isModelType", isModelType)
				.toString();
	}
	
	@Override
	public boolean equals(Object that) {
		if (this == that) {
			return true;
		}
		if (that instanceof FieldInfo) {
			FieldInfo other = (FieldInfo) that;
			return name.equals(other.name);
		}
		
		return false;
	}
	
	public static class Builder {
		
		private String name;
		private String description;
		private String type;
		private JsonType jsonType;
		private Boolean isMultiple;
		private Boolean isModelType;
		
		private Builder() { }
		
		public FieldInfo build() {
			return new FieldInfo(name, description, type, jsonType, isMultiple, isModelType);
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

        public Builder withIsMultiple(boolean isMultiple) {
            this.isMultiple = isMultiple;
            return this;
        }

        public Builder withIsModelType(boolean isModelType) {
            this.isModelType = isModelType;
            return this;
        }
	}
}
