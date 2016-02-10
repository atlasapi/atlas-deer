package org.atlasapi.application.model.auth;

import java.util.UUID;

import com.metabroadcast.common.social.model.UserRef.UserNamespace;

import com.google.common.base.Optional;

/**
 * This class stores requests for tokens, the calling app will have to access the auth url for the
 * provider and respond with a verifier
 *
 * @author liam
 */
public interface TokenRequestStore {

    /**
     * Stores a request for oauth authentication
     **/
    void store(OAuthRequest oauthRequest);

    /**
     * Looks up an OAuthRequest by public token and namespace. The token will be removed from the
     * store if found. Returns absent if not found
     */
    Optional<OAuthRequest> lookupAndRemove(UserNamespace namespace, String token);

    /**
     * Looks up Oauth request by unique identifier
     */
    Optional<OAuthRequest> lookupAndRemove(UUID uuid);
}
