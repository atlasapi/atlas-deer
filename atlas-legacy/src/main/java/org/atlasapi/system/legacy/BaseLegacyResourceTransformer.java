package org.atlasapi.system.legacy;

import org.atlasapi.entity.Identifiable;

import com.google.common.collect.Iterables;

public abstract class BaseLegacyResourceTransformer<F, T extends Identifiable> implements
		LegacyResourceTransformer<F, T> {

	@Override
	public final Iterable<T> transform(Iterable<F> legacy) {
		return Iterables.transform(legacy, this);
	}

}
