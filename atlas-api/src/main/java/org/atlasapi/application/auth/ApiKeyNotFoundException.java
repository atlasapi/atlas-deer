package org.atlasapi.application.auth;

// TODO make this extend QueryExecutionException after reorg
public class ApiKeyNotFoundException extends Exception {
    private static final long serialVersionUID = -8204400513571208163L;
    private final String apiKey;
    
    public ApiKeyNotFoundException(String apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public String getMessage() {
        return "API key not found: " + apiKey;
    }
}
