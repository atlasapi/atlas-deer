package org.atlasapi.content;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;

public enum SortKey {

    ADAPTER("95") {
        @Override
        protected String generateFrom(Item item) {
            if (item.sortKey() != null) {
                return prefix + item.sortKey();
            }
            return null;
        }
    },

    SERIES_EPISODE("85") {
        @Override
        protected String generateFrom(Item item) {
            if (item instanceof Episode) {
                Episode episode = (Episode) item;
                if (episode.getEpisodeNumber() != null && episode.getSeriesNumber() != null) {
                    return SortKey.SERIES_EPISODE.append(String.format("%06d%06d",
                            episode.getSeriesNumber(),
                            episode.getEpisodeNumber()));
                }
            }
            return null;
        }
    },
    BROADCAST("75") {
        @Override
        protected String generateFrom(Item item) {
            Iterator<Broadcast> broadcasts = item.getBroadcasts().iterator();
            if (broadcasts.hasNext()) {
                DateTime firstBroadcast = broadcasts.next().getTransmissionTime();
                while (broadcasts.hasNext()) {
                    DateTime transmissionTime = broadcasts.next().getTransmissionTime();
                    if (transmissionTime.isBefore(firstBroadcast)) {
                        firstBroadcast = transmissionTime;
                    }
                }
                return BROADCAST.append(String.format("%019d", firstBroadcast.getMillis()));
            }
            return null;
        }
    },
    EPISODE("55") {
        @Override
        protected String generateFrom(Item item) {
            if (item instanceof Episode) {
                Episode episode = (Episode) item;
                if (episode.getEpisodeNumber() != null) {
                    return EPISODE.append(String.format("%010d", episode.getEpisodeNumber()));
                }
            }
            return null;
        }
    },
    DEFAULT("11") {
        @Override
        protected String generateFrom(Item item) {
            return this.prefix;
        }

    };

    protected String prefix;

    SortKey(String prefix) {
        this.prefix = prefix;
    }

    protected abstract String generateFrom(Item item);

    private String append(String key) {
        return prefix + key;
    }

    public static String keyFrom(Item item) {
        for (SortKey sortKey : SortKey.values()) {
            String key = sortKey.generateFrom(item);
            if (key != null) {
                return key;
            }
        }
        return "11";
    }

    public static class SortKeyOutputComparator implements Comparator<String> {

        @Override
        public int compare(String sk1, String sk2) {

            if (Strings.isNullOrEmpty(sk1)) {
                sk1 = DEFAULT.prefix;
            }
            if (Strings.isNullOrEmpty(sk2)) {
                sk2 = DEFAULT.prefix;
            }

            if (sk1.equals("99")) {
                return 1;
            }

            if (sk2.equals("99")) {
                return -1;
            }

            sk1 = prefixMap.containsKey(keyPrefix(sk1)) ? transformPrefix(sk1) : sk1;
            sk2 = prefixMap.containsKey(keyPrefix(sk2)) ? transformPrefix(sk2) : sk2;

            return sk2.compareTo(sk1);
        }

        private static final Map<String, String> prefixMap = ImmutableMap.of(
                "99", "11",
                "10", "95",
                "20", "85",
                "30", "75"
        );

        public String transformPrefix(String input) {
            return prefixMap.get(keyPrefix(input)) + input.substring(2);
        }

        public String keyPrefix(String input) {
            return input.substring(0, 2);
        }

    }
}
