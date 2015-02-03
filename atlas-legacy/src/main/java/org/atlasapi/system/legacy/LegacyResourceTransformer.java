package org.atlasapi.system.legacy;

import org.atlasapi.entity.Identifiable;

import com.google.common.base.Function;

public interface LegacyResourceTransformer<F, T extends Identifiable> extends Function<F, T> {

	Iterable<T> transform(Iterable<F> legacy);
	
}
