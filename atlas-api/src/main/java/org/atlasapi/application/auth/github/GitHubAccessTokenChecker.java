package org.atlasapi.application.auth.github;

import java.io.IOException;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.social.auth.credentials.AuthToken;
import com.metabroadcast.common.social.auth.facebook.AccessTokenChecker;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;
import com.metabroadcast.common.social.user.ApplicationIdAwareUserRefBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class GitHubAccessTokenChecker implements AccessTokenChecker {

    private final ApplicationIdAwareUserRefBuilder userRefBuilder;
    private final GitHubAuthClient gitHubClient;

    private static final Logger log = LoggerFactory.getLogger(GitHubAccessTokenChecker.class);

    public GitHubAccessTokenChecker(ApplicationIdAwareUserRefBuilder userRefBuilder,
            GitHubAuthClient gitHubClient) {
        super();
        this.userRefBuilder = checkNotNull(userRefBuilder);
        this.gitHubClient = checkNotNull(gitHubClient);
    }

    @Override
    public Maybe<UserRef> check(AuthToken accessToken) {
        checkArgument(accessToken.isFor(UserNamespace.GITHUB));
        try {
            GitHubUser user = gitHubClient.getUserForToken(accessToken);
            return Maybe.just(userRefBuilder.from(
                    String.valueOf(user.getId()),
                    UserNamespace.GITHUB
            ));
        } catch (IOException e) {
            log.error("Could not check GitHub user", e);
            return Maybe.nothing();
        }
    }

}
