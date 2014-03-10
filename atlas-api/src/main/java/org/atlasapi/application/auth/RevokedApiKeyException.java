package org.atlasapi.application.auth;

// TODO make this extend QueryExecutionException after reorg
public class RevokedApiKeyException extends Exception {
    private static final long serialVersionUID = -8204400513571208163L;
    private final String apiKey;
    
    public RevokedApiKeyException(String apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public String getMessage() {
        return "Revoked API key: " + apiKey;
    }
}
