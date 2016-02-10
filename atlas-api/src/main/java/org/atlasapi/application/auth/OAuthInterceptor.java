package org.atlasapi.application.auth;

import java.io.IOException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.users.User;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.NotAuthenticatedException;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.UnsupportedFormatException;
import org.atlasapi.output.UserProfileIncompleteException;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import static com.google.common.base.Preconditions.checkNotNull;

public class OAuthInterceptor extends HandlerInterceptorAdapter {

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final UserFetcher userFetcher;
    private final NumberToShortStringCodec idCodec;
    private final Set<String> exemptions;
    private final Set<String> urlsToProtect;
    private final Set<String> urlsNotNeedingCompleteProfile;
    private static final Logger log = LoggerFactory.getLogger(OAuthInterceptor.class);

    private OAuthInterceptor(UserFetcher userFetcher,
            NumberToShortStringCodec idCodec,
            Set<String> urlsToProtect,
            Set<String> urlsNotNeedingCompleteProfile,
            Set<String> exemptions) {
        this.userFetcher = userFetcher;
        this.idCodec = idCodec;
        this.urlsToProtect = ImmutableSet.copyOf(urlsToProtect);
        this.urlsNotNeedingCompleteProfile = ImmutableSet.copyOf(urlsNotNeedingCompleteProfile);
        this.exemptions = ImmutableSet.copyOf(exemptions);
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response,
            Object handler) throws Exception {

        Optional<User> user = userFetcher.userFor(request);
        String uri = request.getRequestURI();

        if (!requiresAuthenticatedUser(uri)) {
            return true;
        }

        if (!user.isPresent()) {
            writeError(request, response, new NotAuthenticatedException());
            return false;
        }

        if (needsProfile(uri, user.get())
                && !hasProfileAndAcceptedLicense(user.get())) {
            writeError(request, response, new UserProfileIncompleteException());
            return false;
        }

        return true;
    }

    private boolean hasProfileAndAcceptedLicense(User user) {
        return user.isProfileComplete() && user.getLicenseAccepted().isPresent();
    }

    private void writeError(HttpServletRequest request, HttpServletResponse response,
            Exception exception)
            throws IOException, UnsupportedFormatException, NotAcceptableException {
        ResponseWriter writer = writerResolver.writerFor(request, response);
        ErrorSummary summary = ErrorSummary.forException(exception);
        new ErrorResultWriter().write(summary, writer, request, response);
    }

    private boolean requiresAuthenticatedUser(String requestUri)
            throws NotAuthenticatedException, UserProfileIncompleteException {
        if (urlsToProtect.isEmpty()) {
            return true;
        }
        boolean protectedUrl = false;
        for (String uri : urlsToProtect) {
            if (requestUri.startsWith(uri)
                    && !protectedUrl) {
                protectedUrl = !exemptions.contains(requestUri);
            }
        }
        return protectedUrl;
    }

    private boolean needsProfile(String uri, User user) {
        String uid = idCodec.encode(user.getId().toBigInteger());
        String requestUri = uriWithoutExtension(uri).replace(uid, ":uid");

        // Make a subset by taking urls to protect and removing urls not needing auth
        for (String urlToProtect : urlsToProtect) {
            if (requestUri.startsWith(urlToProtect)) {
                return !exemptions.contains(requestUri) && !urlsNotNeedingCompleteProfile.contains(
                        requestUri);
            }
        }
        return false;
    }

    private String uriWithoutExtension(String requestUri) {
        int suffixStart = requestUri.lastIndexOf(".");
        if (suffixStart >= 0) {
            return requestUri.substring(0, suffixStart);
        }
        return requestUri;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private UserFetcher userFetcher;
        private NumberToShortStringCodec idCodec;
        private Set<String> exemptions = ImmutableSet.of();
        private Set<String> urlsToProtect = ImmutableSet.of();
        private Set<String> urlsNotNeedingCompleteProfile = ImmutableSet.of();

        public Builder withUserFetcher(UserFetcher userFetcher) {
            this.userFetcher = userFetcher;
            return this;
        }

        public Builder withIdCodec(NumberToShortStringCodec idCodec) {
            this.idCodec = idCodec;
            return this;
        }

        public Builder withExemptions(Set<String> exemptions) {
            this.exemptions = exemptions;
            return this;
        }

        public Builder withUrlsToProtect(Set<String> urlsToProtect) {
            this.urlsToProtect = urlsToProtect;
            return this;
        }

        public Builder withUrlsNotNeedingCompleteProfile(
                Set<String> urlsNotNeedingCompleteProfile) {
            this.urlsNotNeedingCompleteProfile = urlsNotNeedingCompleteProfile;
            return this;
        }

        public OAuthInterceptor build() {
            checkNotNull(userFetcher);
            checkNotNull(idCodec);
            if (urlsToProtect.isEmpty()) {
                log.info("No protected URLs have been set");
            }
            return new OAuthInterceptor(
                    this.userFetcher,
                    this.idCodec,
                    this.urlsToProtect,
                    this.urlsNotNeedingCompleteProfile,
                    this.exemptions
            );
        }
    }
}
