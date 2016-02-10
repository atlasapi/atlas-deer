package org.atlasapi.entity.util;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkNotNull;

public class RuntimeWriteException extends RuntimeException {

    public RuntimeWriteException(@Nonnull WriteException cause) {
        super(checkNotNull(cause));
    }

    @Override
    public synchronized WriteException getCause() {
        return (WriteException) super.getCause();
    }

}
