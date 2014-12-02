package org.atlasapi.generation.parsing;

import javax.annotation.Nullable;

public class StandardJavadocParser implements JavadocParser {

	@Override
	public String parse(@Nullable String rawDocString) {
		if (rawDocString == null) {
			return "";
		}
		return rawDocString.replace("\n", "");
	}

}
