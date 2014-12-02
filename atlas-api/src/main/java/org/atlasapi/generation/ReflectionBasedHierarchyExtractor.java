package org.atlasapi.generation;

import static org.elasticsearch.common.Preconditions.checkNotNull;

import java.util.Set;

import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class ReflectionBasedHierarchyExtractor implements HierarchyExtractor {
	
	private Types typeUtils;
	
	@Override
	public final void init(Types typeUtils) {
		this.typeUtils = checkNotNull(typeUtils);
	}
	

	@Override
	public Set<TypeElement> fullHierarchy(TypeElement type) {
		return ImmutableSet.copyOf(Iterables.transform(
				hierarchy(type.asType()), 
				new Function<TypeMirror, TypeElement>() {
					@Override
					public TypeElement apply(TypeMirror input) {
						return (TypeElement) typeUtils.asElement(input);
					}
				}
		));
	}

	private Iterable<? extends TypeMirror> hierarchy(TypeMirror type) {
		Iterable<? extends TypeMirror> directSupertypes = typeUtils.directSupertypes(type);
		if (Iterables.isEmpty(directSupertypes)) {
			return directSupertypes;
		}
		
		return Iterables.concat(
				FluentIterable.from(directSupertypes)
					.transformAndConcat(fetchHierarchy()), 
				Sets.newHashSet(type)
		);
	}


	private Function<TypeMirror, Iterable<? extends TypeMirror>> fetchHierarchy() {
		return new Function<TypeMirror, Iterable<? extends TypeMirror>>(){
			@Override
			public Iterable<? extends TypeMirror> apply(TypeMirror input) {
				return hierarchy(input);
			}
		};
	}
}
