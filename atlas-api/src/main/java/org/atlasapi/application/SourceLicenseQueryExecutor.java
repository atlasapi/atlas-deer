package org.atlasapi.application;

import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;

import com.google.common.base.Optional;

public class SourceLicenseQueryExecutor implements UserAwareQueryExecutor<SourceLicense> {

    private final SourceIdCodec sourceIdCodec;
    private final SourceLicenseStore store;

    public SourceLicenseQueryExecutor(SourceIdCodec sourceIdCodec, SourceLicenseStore store) {
        super();
        this.sourceIdCodec = sourceIdCodec;
        this.store = store;
    }

    @Override
    public UserAwareQueryResult<SourceLicense> execute(UserAwareQuery<SourceLicense> query)
            throws QueryExecutionException {
        return query.isListQuery() ? multipleQuery(query) : singleQuery(query);
    }

    private UserAwareQueryResult<SourceLicense> singleQuery(UserAwareQuery<SourceLicense> query)
            throws NotFoundException {
        Optional<Publisher> source = sourceIdCodec.decode(query.getOnlyId());
        // TODO CHECK PERMISSION
        if (source.isPresent()) {
            Optional<SourceLicense> license = store.licenseFor(source.get());
            if (license.isPresent()) {
                return UserAwareQueryResult.singleResult(license.get(), query.getContext());
            }
        }
        // If we get to here then there was no license for the given id
        throw new NotFoundException(query.getOnlyId());
    }

    private UserAwareQueryResult<SourceLicense> multipleQuery(UserAwareQuery<SourceLicense> query) {
        throw new UnsupportedOperationException();
    }

}
