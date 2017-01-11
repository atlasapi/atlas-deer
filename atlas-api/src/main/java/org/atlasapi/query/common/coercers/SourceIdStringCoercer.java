package org.atlasapi.query.common.coercers;

import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class SourceIdStringCoercer extends AbstractAttributeCoercer<Publisher> {

    private final SourceIdCodec sourceIdCodec;

    private SourceIdStringCoercer(SourceIdCodec sourceIdCodec) {
        this.sourceIdCodec = checkNotNull(sourceIdCodec);
    }

    public static SourceIdStringCoercer create(SourceIdCodec sourceIdCodec) {
        return new SourceIdStringCoercer(sourceIdCodec);
    }

    @Override
    protected Publisher coerce(String input) {
        Optional<Publisher> publisher = sourceIdCodec.decode(input);

        if (publisher.isPresent()) {
            return publisher.get();
        } else {
            return null;
        }
    }
}
