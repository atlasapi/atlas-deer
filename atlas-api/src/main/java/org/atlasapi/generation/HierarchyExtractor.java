package org.atlasapi.generation;

import java.util.Set;

import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Types;

// TODO naming: ancestor extractor?
public interface HierarchyExtractor {

	void init(Types typeUtils);
	
	/**
	 * For a given type, recursively obtain super-types until no more are found
	 * @param type
	 * @return
	 */
	Set<TypeElement> fullHierarchy(TypeElement type);
}
