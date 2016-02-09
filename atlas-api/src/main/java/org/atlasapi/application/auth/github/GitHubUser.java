package org.atlasapi.application.auth.github;

import com.google.api.client.util.Key;

public class GitHubUser {

    @Key("login")
    private String login;
    @Key("id")
    private Integer id;
    @Key("avatar_url")
    private String avatarUrl;
    @Key("url")
    private String url;
    @Key("html_url")
    private String htmlUrl;
    @Key("name")
    private String name;
    @Key("company")
    private String company;
    @Key("blog")
    private String blog;

    public String getLogin() {
        return login;
    }

    public Integer getId() {
        return id;
    }

    public String getAvatarUrl() {
        return avatarUrl;
    }

    public String getUrl() {
        return url;
    }

    public String getHtmlUrl() {
        return htmlUrl;
    }

    public String getName() {
        return name;
    }

    public String getCompany() {
        return company;
    }

    public String getBlog() {
        return blog;
    }
}
