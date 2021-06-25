package org.atlasapi.www.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class RequestLoggingFilter extends OncePerRequestFilter {
    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);

    @Override
    protected void doFilterInternal(
            HttpServletRequest req,
            HttpServletResponse resp,
            FilterChain chain
    ) throws IOException, ServletException {
        // Hack to allow extraction of the status code. In newer versions of javax.servlet-api (3.0+) this will no
        // longer be necessary.
        StatusExposingServletResponse statusExposingServletResponse = new StatusExposingServletResponse(resp);
        Instant start = Instant.now();
        try {
            chain.doFilter(req, statusExposingServletResponse);
        } finally {
            Instant finish = Instant.now();
            long time = Duration.between(start, finish).toMillis();
            String fullRequestURI = fullRequestURI(req);
            log.info("{} {} {} {}ms", req.getMethod(), fullRequestURI, statusExposingServletResponse.getStatus(), time);
        }
    }

    private String fullRequestURI(HttpServletRequest request) {
        StringBuilder requestURL = new StringBuilder(request.getRequestURI());
        String queryString = request.getQueryString();

        if (queryString == null) {
            return requestURL.toString();
        } else {
            return requestURL.append('?').append(queryString).toString();
        }
    }

    private static class StatusExposingServletResponse extends HttpServletResponseWrapper {
        private int httpStatus = SC_OK;

        public StatusExposingServletResponse(HttpServletResponse response) {
            super(response);
        }

        public int getStatus() {
            return httpStatus;
        }

        @Override
        public void setStatus(int sc) {
            httpStatus = sc;
            super.setStatus(sc);
        }

        @Override
        public void setStatus(int status, String string) {
            super.setStatus(status, string);
            this.httpStatus = status;
        }

        @Override
        public void sendError(int sc) throws IOException {
            httpStatus = sc;
            super.sendError(sc);
        }

        @Override
        public void sendError(int sc, String msg) throws IOException {
            httpStatus = sc;
            super.sendError(sc, msg);
        }

        @Override
        public void sendRedirect(String location) throws IOException {
            httpStatus = SC_MOVED_TEMPORARILY;
            super.sendRedirect(location);
        }

    }
}
