package org.atlasapi.query.common;

import java.util.Iterator;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import org.apache.commons.lang.StringUtils;

import static com.google.common.base.Preconditions.checkNotNull;

public class ReplacementSuggestion {

    private static final int MAX_DISTANCE = 3;
    private static final String NO_REPLACEMENT = null;

    private final Set<String> validCandidates;
    private final String prefix;
    private final String replacementPattern;
    private final Function<String, String> invalidTransformer;
    private final Function<String, String> replacementTransformer;

    public ReplacementSuggestion(Iterable<String> validCandidates, String prefix,
            String replacementPattern) {
        this(validCandidates, prefix, replacementPattern,
                Functions.<String>identity(), Functions.<String>identity()
        );
    }

    public ReplacementSuggestion(Iterable<String> validCandidates, String prefix,
            String replacementPattern,
            Function<String, String> invalidTransformer,
            Function<String, String> replacementTransformer) {
        this.validCandidates = ImmutableSet.copyOf(validCandidates);
        this.prefix = checkNotNull(prefix);
        this.replacementPattern = checkNotNull(replacementPattern);
        this.invalidTransformer = checkNotNull(invalidTransformer);
        this.replacementTransformer = checkNotNull(replacementTransformer);
    }

    public String forInvalid(Iterable<String> invalid) {
        StringBuilder msg = new StringBuilder(prefix);
        Iterator<String> iter = Ordering.natural().sortedCopy(invalid).iterator();
        if (iter.hasNext()) {
            appendInvalidName(msg, iter.next());
            while (iter.hasNext()) {
                msg.append(", ");
                appendInvalidName(msg, iter.next());
            }
        }
        return msg.toString();
    }

    private void appendInvalidName(StringBuilder msg, String invalid) {
        msg.append(invalid);
        String replacement = findReplacement(invalidTransformer.apply(invalid), validCandidates);
        if (replacement != NO_REPLACEMENT) {
            msg.append(String.format(
                    replacementPattern,
                    replacementTransformer.apply(replacement)
            ));
        }
    }

    private String findReplacement(String invalid, Set<String> validParams) {
        for (String valid : validParams) {
            int distance = StringUtils.getLevenshteinDistance(valid, invalid);
            if (distance < MAX_DISTANCE) {
                return valid;
            }
        }
        return NO_REPLACEMENT;
    }

}
