package org.atlasapi.query.v4.schedule;

import static com.google.common.base.Preconditions.checkArgument;
import static com.metabroadcast.common.webapp.query.DateTimeInQueryParser.queryDateTimeParser;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import com.google.common.collect.Range;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.auth.ApplicationSourcesFetcher;
import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.annotation.ContextualAnnotationsExtractor;
import org.atlasapi.query.common.InvalidAnnotationException;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryParseException;
import org.atlasapi.query.common.SetBasedRequestParameterValidator;
import org.atlasapi.source.Sources;
import org.elasticsearch.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.webapp.query.DateTimeInQueryParser;

class ScheduleRequestParser {
    
    private static final Pattern CHANNEL_ID_PATTERN = Pattern.compile(
        ".*schedules/([^.]+)(.[\\w\\d.]+)?$"
    );
    private static final Splitter commaSplitter = Splitter.on(",").omitEmptyStrings().trimResults();

    private static final Range<Integer> COUNT_RANGE = Range.closed(1, 10);
    
    private final ApplicationSourcesFetcher applicationFetcher;

    private final SetBasedRequestParameterValidator singleValidator;
    private final SetBasedRequestParameterValidator multiValidator;
    
    private final NumberToShortStringCodec idCodec;
    private final DateTimeInQueryParser dateTimeParser;
    private final ContextualAnnotationsExtractor annotationExtractor;
    private final Duration maxQueryDuration;
    private final Clock clock;

    public ScheduleRequestParser(ApplicationSourcesFetcher appFetcher, Duration maxQueryDuration, Clock clock, ContextualAnnotationsExtractor annotationsExtractor) {
        this.applicationFetcher = appFetcher;
        this.maxQueryDuration = maxQueryDuration;
        this.idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.dateTimeParser = queryDateTimeParser()
                .parsesIsoDateTimes()
                .parsesIsoTimes()
                .parsesIsoDates()
                .parsesOffsets()
                .build();
        this.clock = clock;
        this.annotationExtractor = annotationsExtractor;
        this.singleValidator = singleRequestValidator(applicationFetcher);
        this.multiValidator = multiRequestValidator(applicationFetcher);
    }

    private SetBasedRequestParameterValidator singleRequestValidator(ApplicationSourcesFetcher fetcher) {
        ImmutableList<String> required = ImmutableList.<String>builder()
            .add("from","source")
            .addAll(fetcher.getParameterNames())
            .build();
        return SetBasedRequestParameterValidator.builder()
            .withRequiredParameters(required.toArray(new String[required.size()]))
            .withOptionalParameters("annotations", "callback")
            .withRequiredAlternativeParameters("to", "count")
            .build();
    }

    private SetBasedRequestParameterValidator multiRequestValidator(ApplicationSourcesFetcher fetcher) {
        ImmutableList<String> required = ImmutableList.<String>builder()
            .add("id", "from", "source")
            .addAll(fetcher.getParameterNames())
            .build();
        return SetBasedRequestParameterValidator.builder()
            .withRequiredParameters(required.toArray(new String[required.size()]))
            .withOptionalParameters("annotations", "callback")
            .withRequiredAlternativeParameters("to", "count")
            .build();
    }

    public ScheduleQuery queryFrom(HttpServletRequest request) throws QueryParseException, InvalidApiKeyException {
        Matcher matcher = CHANNEL_ID_PATTERN.matcher(request.getRequestURI());
        return matcher.matches() ? parseSingleRequest(request)
                                 : parseMultiRequest(request);
    }

    private ScheduleQuery parseSingleRequest(HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        singleValidator.validateParameters(request);

        Publisher publisher = extractPublisher(request);
        QueryContext context = parseContext(request, publisher);
        Id channel = extractChannel(request);

        DateTime from = extractFrom(request);
        Optional<DateTime> to = extractTo(request);
        if (to.isPresent()) {
            checkInterval(from, to.get());
            return ScheduleQuery.single(publisher, from, to.get(), context, channel);
        }

        Integer count = extractCount(request);
        return ScheduleQuery.single(publisher, from, count, context, channel);
    }

    private ScheduleQuery parseMultiRequest(HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        
        multiValidator.validateParameters(request);
        
        Publisher publisher = extractPublisher(request);
        QueryContext context = parseContext(request, publisher);
        List<Id> channels = extractChannels(request);

        DateTime from = extractFrom(request);
        Optional<DateTime> to = extractTo(request);
        if (to.isPresent()) {
            checkInterval(from, to.get());
            return ScheduleQuery.multi(publisher, from, to.get(), context, channels);
        }

        Integer count = extractCount(request);
        return ScheduleQuery.multi(publisher, from, count, context, channels);
    }


    private QueryContext parseContext(HttpServletRequest request, Publisher publisher)
            throws InvalidApiKeyException, InvalidAnnotationException {
        ApplicationSources appSources = getConfiguration(request);
        
        checkArgument(appSources.isReadEnabled(publisher), "Source %s not enabled", publisher);
        
        ActiveAnnotations annotations = annotationExtractor.extractFromRequest(request);
        QueryContext context = new QueryContext(appSources, annotations, request);
        return context;
    }

    private List<Id> extractChannels(HttpServletRequest request) throws QueryParseException {
        String csvCids = request.getParameter("id");
        List<String> cids = commaSplitter.splitToList(csvCids);
        List<String> invalidIds = Lists.newLinkedList();
        ImmutableList.Builder<Id> ids = ImmutableList.builder();
        for (String cid : cids) {
            try {
                ids.add(transformToId(cid));
            } catch (QueryParseException qpe) {
                invalidIds.add(cid);
            }
        }
        if (!invalidIds.isEmpty()) {
            throw new QueryParseException(String.format("Invalid id%s %s", 
                    invalidIds.size()>1 ? "s" : "" , Joiner.on(", ").join(invalidIds)));
        }
        return ids.build();
    }
    
    private Id extractChannel(HttpServletRequest request) throws QueryParseException {
        Matcher matcher = CHANNEL_ID_PATTERN.matcher(request.getRequestURI());
        // we already know that this matches so we'll never get null
        String channelId = matcher.matches() ? matcher.group(1) : null;
        return transformToId(channelId);
    }

    private Id transformToId(String channelId) throws QueryParseException {
        try {
            return Id.valueOf(idCodec.decode(channelId));
        } catch (IllegalArgumentException e) {
            throw new QueryParseException("Invalid id " + channelId);
        }
    }

    private DateTime extractFrom(HttpServletRequest request) {
        DateTime now = clock.now();
        DateTime from = dateTimeParser.parse(getParameter(request, "from"), now);
        return from;
    }

    private Optional<DateTime> extractTo(HttpServletRequest request) {
        DateTime now = clock.now();
        String toParam = request.getParameter("to");
        if (Strings.isNullOrEmpty(toParam)) {
            return Optional.absent();
        }
        DateTime from = dateTimeParser.parse(toParam, now);
        return Optional.of(from);
    }

    private void checkInterval(DateTime from, DateTime to) {
        Interval queryInterval = new Interval(from, to);
        checkArgument(!queryInterval.toDuration().isLongerThan(maxQueryDuration), "Query interval cannot be longer than %s", maxQueryDuration);
    }

    private Integer extractCount(HttpServletRequest request) {
        Integer count = Integer.valueOf(getParameter(request, "count"));

        checkArgument(COUNT_RANGE.contains(count),  "'count' must be in range %s, was %s", COUNT_RANGE, count);
        return count;
    }

    private Publisher extractPublisher(HttpServletRequest request) {
        String pubKey = getParameter(request, "source");
        Optional<Publisher> publisher = Sources.fromPossibleKey(pubKey);
        checkArgument(publisher.isPresent(), "Unknown source %s", pubKey);
        return publisher.get();
    }

    private ApplicationSources getConfiguration(HttpServletRequest request) throws InvalidApiKeyException {
        Optional<ApplicationSources> config = applicationFetcher.sourcesFor(request);
        if (config.isPresent()) {
            return config.get();
        }
        // key is required parameter so we should never reach here.
        throw new IllegalStateException("application not fetched");
    }

    private String getParameter(HttpServletRequest request, String param) {
        String paramValue = request.getParameter(param);
        checkArgument(!Strings.isNullOrEmpty(paramValue), "Missing required parameter %s", param);
        return paramValue;
    }

}
