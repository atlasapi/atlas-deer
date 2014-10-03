package org.atlasapi.meta.annotations.model;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public class FieldInfo {
	
	private final String name;
	private final String description;
	private final String type;
	private final Optional<String> endpoint;
	
	public static Builder builder() {
		return new Builder();
	}
	
	private FieldInfo(String name, String description, String type, Optional<String> endpoint) {
		this.name = Preconditions.checkNotNull(name);
		this.description = description;
		this.type = type;
		this.endpoint = endpoint;
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
	
	public Optional<String> endpoint() {
		return endpoint;
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
				.add("endpoint", endpoint)
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
		private Optional<String> endpoint = Optional.absent();
		
		private Builder() { }
		
		public FieldInfo build() {
			return new FieldInfo(name, description, type, endpoint);
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
		
		public Builder withEndpoint(String endpoint) {
			this.endpoint = Optional.fromNullable(endpoint);
			return this;
		}
	}
}
