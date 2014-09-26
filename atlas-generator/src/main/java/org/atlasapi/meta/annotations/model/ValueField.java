package org.atlasapi.meta.annotations.model;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class ValueField {
	
	private final String name;
	private final String description;
	private final Boolean isDefault;
	private final String value;
	
	public static Builder builder() {
		return new Builder();
	}
	
	public ValueField(String name, String description, Boolean isDefault, String value) {
		this.name = Preconditions.checkNotNull(name);
		this.description = description;
		this.isDefault = isDefault;
		this.value = value;
	}

	public String name() {
		return name;
	}

	public String description() {
		return description;
	}

	public Boolean isDefault() {
		return isDefault;
	}

	public String value() {
		return value;
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(name);
	}
	
	@Override
	public String toString() {
		return Objects.toStringHelper(ValueField.class)
				.add("name", name)
				.add("description", description)
				.add("isDefault", isDefault)
				.add("value", value)
				.toString();
	}
	
	@Override
	public boolean equals(Object that) {
		if (this == that) {
			return true;
		}
		if (that instanceof ValueField) {
			ValueField other = (ValueField) that;
			return name.equals(other.name);
		}
		
		return false;
	}
	
	public static class Builder {
		
		private String name;
		private String description;
		private Boolean isDefault;
		private String value;
		
		private Builder() { }
		
		public ValueField build() {
			return new ValueField(name, description, isDefault, value);
		}

		public Builder withName(String name) {
			this.name = name;
			return this;
		}

		public Builder withDescription(String description) {
			this.description = description;
			return this;
		}

		public Builder withIsDefault(Boolean isDefault) {
			this.isDefault = isDefault;
			return this;
		}

		public Builder withValue(String value) {
			this.value = value;
			return this;
		}
	}
}
