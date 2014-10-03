package org.atlasapi.meta.annotations.model;

import static com.google.common.base.Preconditions.checkNotNull;

import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.base.Objects;

public class Operation {
	
	private final RequestMethod method;
	private final String path;
	
	public Operation(RequestMethod method, String path) {
		this.method = checkNotNull(method);
		this.path = checkNotNull(path);
	}
	
	public RequestMethod method() {
		return method;
	}
	
	public String path() {
		return path;
	}
	
	@Override
	public boolean equals(Object that) {
		if (this == that) {
			return true;
		}
		if (that instanceof Operation) {
			Operation other = (Operation) that;
			return method.equals(other.method)
					&& path.equals(other.path);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(method, path);
	}
}