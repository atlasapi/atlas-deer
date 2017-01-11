package org.atlasapi.application;

import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.query.common.useraware.UserAccountsAwareQuery;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryExecutor;

import com.google.common.base.Optional;

public class SourceLicenseQueryExecutorMultipleAccounts implements
        UserAccountsAwareQueryExecutor<SourceLicense> {

    private final SourceIdCodec sourceIdCodec;
    private final SourceLicenseStore store;

    public SourceLicenseQueryExecutorMultipleAccounts(SourceIdCodec sourceIdCodec,
            SourceLicenseStore store) {
        super();
        this.sourceIdCodec = sourceIdCodec;
        this.store = store;
    }

    @Override
    public UserAccountsAwareQueryResult<SourceLicense> execute(
            UserAccountsAwareQuery<SourceLicense> query)
            throws QueryExecutionException {
        return query.isListQuery() ? multipleQuery(query) : singleQuery(query);
    }

    private UserAccountsAwareQueryResult<SourceLicense> singleQuery(
            UserAccountsAwareQuery<SourceLicense> query)
            throws NotFoundException {
        Optional<Publisher> source = sourceIdCodec.decode(query.getOnlyId());
        // TODO CHECK PERMISSION
        if (source.isPresent()) {
            Optional<SourceLicense> license = store.licenseFor(source.get());
            if (license.isPresent()) {
                return UserAccountsAwareQueryResult.singleResult(license.get(), query.getContext());
            }
        }
        // If we get to here then there was no license for the given id
        throw new NotFoundException(query.getOnlyId());
    }

    private UserAccountsAwareQueryResult<SourceLicense> multipleQuery(
            UserAccountsAwareQuery<SourceLicense> query) {
        throw new UnsupportedOperationException();
    }

}
