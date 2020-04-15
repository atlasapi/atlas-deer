package org.atlasapi.query.v4.schedule;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.webapp.query.DateTimeInQueryParser;
import org.atlasapi.application.ApplicationFetcher;
import org.atlasapi.application.ApplicationResolutionException;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.annotation.ContextualAnnotationsExtractor;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.InvalidAnnotationException;
import org.atlasapi.query.common.exceptions.MissingAnnotationException;
import org.atlasapi.query.common.validation.SetBasedRequestParameterValidator;
import org.atlasapi.source.Sources;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.metabroadcast.common.webapp.query.DateTimeInQueryParser.queryDateTimeParser;

class ScheduleRequestParser {

    private static final Pattern CHANNEL_ID_PATTERN = Pattern.compile(
            ".*schedules/([^.]+)(.[\\w\\d.]+)?$"
    );
    private static final Splitter commaSplitter = Splitter.on(",").omitEmptyStrings().trimResults();

    private static final Range<Integer> COUNT_RANGE = Range.closed(1, 10);

    private static final String FROM_PARAM = "from";
    private static final String TO_PARAM = "to";
    private static final String COUNT_PARAM = "count";
    private static final String SOURCE_PARAM = "source";
    private static final String OVERRIDE_SOURCE_PARAM = "override_source";
    private static final String ANNOTATIONS_PARAM = "annotations";
    private static final String CALLBACK_PARAM = "callback";
    private static final String ID_PARAM = "id";
    private static final String ORDER_BY = "order_by";

    private final ApplicationFetcher applicationFetcher;

    private final SetBasedRequestParameterValidator singleValidator;
    private final SetBasedRequestParameterValidator multiValidator;

    private final NumberToShortStringCodec idCodec;
    private final DateTimeInQueryParser dateTimeParser;
    private final ContextualAnnotationsExtractor annotationExtractor;
    private final Duration maxQueryDuration;
    private final Clock clock;

    public ScheduleRequestParser(
            ApplicationFetcher appFetcher,
            Duration maxQueryDuration,
            Clock clock,
            ContextualAnnotationsExtractor annotationsExtractor
    ) {
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

    private SetBasedRequestParameterValidator singleRequestValidator(
            ApplicationFetcher fetcher
    ) {
        ImmutableList<String> required = ImmutableList.<String>builder()
                .add(FROM_PARAM, SOURCE_PARAM)
                .addAll(fetcher.getParameterNames())
                .build();

        return SetBasedRequestParameterValidator.builder()
            .withRequiredParameters(required.toArray(new String[required.size()]))
            .withOptionalParameters(
                    ANNOTATIONS_PARAM, CALLBACK_PARAM, ORDER_BY, OVERRIDE_SOURCE_PARAM)
            .withRequiredAlternativeParameters(TO_PARAM, COUNT_PARAM)
            .build();
    }

    private SetBasedRequestParameterValidator multiRequestValidator(
            ApplicationFetcher fetcher
    ) {
        ImmutableList<String> required = ImmutableList.<String>builder()
                .add(ID_PARAM, FROM_PARAM, SOURCE_PARAM)
                .addAll(fetcher.getParameterNames())
                .build();
        return SetBasedRequestParameterValidator.builder()
            .withRequiredParameters(required.toArray(new String[required.size()]))
            .withOptionalParameters(
                    ANNOTATIONS_PARAM, CALLBACK_PARAM, ORDER_BY, OVERRIDE_SOURCE_PARAM)
            .withRequiredAlternativeParameters(TO_PARAM, COUNT_PARAM)
            .build();
    }

    public ScheduleQuery queryFrom(HttpServletRequest request)
            throws QueryParseException, ApplicationResolutionException {
        Matcher matcher = CHANNEL_ID_PATTERN.matcher(request.getRequestURI());
        return matcher.matches() ? parseSingleRequest(request)
                                 : parseMultiRequest(request);
    }

    private ScheduleQuery parseSingleRequest(HttpServletRequest request)
            throws QueryParseException, ApplicationResolutionException {
        singleValidator.validateParameters(request);

        Publisher publisher = extractPublisher(request);
        QueryContext context = parseContext(request, publisher);
        Id channel = extractChannel(request);

        DateTime now = clock.now();
        DateTime from = extractFrom(request, now);
        Optional<Publisher> override = extractOverride(request);

        ScheduleQuery.Builder builder = ScheduleQuery.builder()
                .withSource(publisher)
                .withStart(from)
                .withContext(context)
                .withId(channel);

        if (override.isPresent()) {
            builder.withOverride(override.get());
        }

        Optional<DateTime> to = extractTo(request, now);
        if (to.isPresent()) {
            checkInterval(from, to.get());

            return builder.withEnd(to.get()).build();
        }

        Integer count = extractCount(request);

        return builder.withCount(count).build();
    }

    private Optional<Publisher> extractOverride(HttpServletRequest request) {
        String param = request.getParameter(OVERRIDE_SOURCE_PARAM);
        if (Strings.isNullOrEmpty(param)) {
            return Optional.empty();
        }

        return Optional.ofNullable(Publisher.fromKey(param).valueOrNull());
    }

    private ScheduleQuery parseMultiRequest(HttpServletRequest request)
            throws QueryParseException, ApplicationResolutionException {

        multiValidator.validateParameters(request);

        Publisher publisher = extractPublisher(request);
        QueryContext context = parseContext(request, publisher);
        List<Id> channels = extractChannels(request);

        DateTime now = clock.now();
        DateTime from = extractFrom(request, now);
        Optional<Publisher> override = extractOverride(request);

        ScheduleQuery.Builder builder = ScheduleQuery.builder()
                .withSource(publisher)
                .withStart(from)
                .withContext(context)
                .withIds(channels);

        if (override.isPresent()) {
            builder.withOverride(override.get());
        }

        Optional<DateTime> to = extractTo(request, now);
        if (to.isPresent()) {
            checkInterval(from, to.get());
            return builder.withEnd(to.get()).build();
        }

        Integer count = extractCount(request);
        return builder.withCount(count).build();
    }

    private QueryContext parseContext(HttpServletRequest request, Publisher publisher)
            throws ApplicationResolutionException,
                InvalidAnnotationException,
                MissingAnnotationException
    {
        Application application = getConfiguration(request);

        checkArgument(application.getConfiguration().isReadEnabled(publisher), "Source %s not enabled", publisher);

        ActiveAnnotations annotations = annotationExtractor.extractFromRequest(request);
        return QueryContext.create(application, annotations, request);
    }

    private List<Id> extractChannels(HttpServletRequest request) throws QueryParseException {
        String csvCids = request.getParameter(ID_PARAM);
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
                    invalidIds.size() > 1 ? "s" : "", Joiner.on(", ").join(invalidIds)
            ));
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

    private DateTime extractFrom(HttpServletRequest request, DateTime now) {
        DateTime from = dateTimeParser.parse(getParameter(request, FROM_PARAM), now);
        return from;
    }

    private Optional<DateTime> extractTo(HttpServletRequest request, DateTime now) {
        String toParam = request.getParameter(TO_PARAM);
        if (Strings.isNullOrEmpty(toParam)) {
            return Optional.empty();
        }
        DateTime from = dateTimeParser.parse(toParam, now);
        return Optional.of(from);
    }

    private void checkInterval(DateTime from, DateTime to) {
        Interval queryInterval = new Interval(from, to);
        checkArgument(
                !queryInterval.toDuration().isLongerThan(maxQueryDuration),
                "Query interval cannot be longer than %s",
                maxQueryDuration
        );
    }

    private Integer extractCount(HttpServletRequest request) {
        Integer count = Integer.valueOf(getParameter(request, COUNT_PARAM));

        checkArgument(
                COUNT_RANGE.contains(count),
                "'count' must be between %s and %s, was %s",
                COUNT_RANGE.lowerEndpoint(),
                COUNT_RANGE.upperEndpoint(),
                count
        );
        return count;
    }

    private Publisher extractPublisher(HttpServletRequest request) {
        String pubKey = getParameter(request, SOURCE_PARAM);
        com.google.common.base.Optional<Publisher> publisher =
                Sources.fromPossibleKey(pubKey);
        checkArgument(publisher.isPresent(), "Unknown source %s", pubKey);
        return publisher.get();
    }

    private Application getConfiguration(HttpServletRequest request) throws
            ApplicationResolutionException {

        Optional<Application> application = applicationFetcher.applicationFor(request);
        if (application.isPresent()) {
            return application.get();
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
