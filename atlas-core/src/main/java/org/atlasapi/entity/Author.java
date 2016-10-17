package org.atlasapi.entity;

import java.util.Objects;

public class Author {
    private final String authorName;
    private final String authorInitials;

    private Author(Builder builder) {
        this.authorName = builder.authorName;
        this.authorInitials = builder.authorInitials;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getAuthorName() {
        return this.authorName;
    }

    public String getAuthorInitials() {
        return this.authorInitials;
    }

    public boolean equals(Object o) {
        if(this == o) {
            return true;
        } else if(!(o instanceof Author)) {
            return false;
        } else {
            Author author = (Author)o;
            return Objects.equals(this.authorName, author.authorName) &&
                    Objects.equals(this.authorInitials, author.authorInitials);
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.authorName, this.authorInitials});
    }

    public Builder copy() {
        return (new Builder())
                .withAuthorName(this.authorName)
                .withAuthorInitials(this.authorInitials);
    }

    public static class Builder {
        private String authorName;
        private String authorInitials;

        private Builder() {
        }

        public Builder withAuthorName(String authorName) {
            this.authorName = authorName;
            return this;
        }

        public Builder withAuthorInitials(String authorInitials) {
            this.authorInitials = authorInitials;
            return this;
        }

        public Author build() {
            return new Author(this);
        }
    }
}
