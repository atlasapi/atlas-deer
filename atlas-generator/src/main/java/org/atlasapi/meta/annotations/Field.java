package org.atlasapi.meta.annotations;

public interface Field {

	String name();
	String description();
	String value();
	// TODO potentially differentiate between JSON field type and actual Java backing Type
	// e.g. JSON: 'text' vs Java: String or JSON: 'number' vs Java: Long
	// could be needed for auto-generation of clients
}
