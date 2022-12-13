package com.lostsidewalk.buffy;

import com.lostsidewalk.buffy.Publisher.PubFormat;
import com.lostsidewalk.buffy.Publisher.PubResult;
import com.lostsidewalk.buffy.feed.FeedDefinition;
import com.lostsidewalk.buffy.feed.FeedDefinitionDao;
import com.lostsidewalk.buffy.post.StagingPost;
import com.lostsidewalk.buffy.post.StagingPostDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.collections4.CollectionUtils.size;
import static org.apache.commons.lang3.time.FastDateFormat.MEDIUM;
import static org.apache.commons.lang3.time.FastDateFormat.getDateTimeInstance;


@Slf4j
@Component
public class PostPublisher {

    @Autowired
    FeedDefinitionDao feedDefinitionDao;

    @Autowired
    StagingPostDao stagingPostDao;

    @Autowired
    List<Publisher> publishers;

    @PostConstruct
    public void postConstruct() {
        log.info("Post publisher constructed, publisherCt={}", size(publishers));
    }

    @SuppressWarnings("unused")
    public List<PubResult> publishFeed(String username, String feedIdent) throws DataAccessException, DataUpdateException {
        FeedDefinition feedDefinition = this.feedDefinitionDao.findByFeedIdent(username, feedIdent);
        if (feedDefinition == null) {
            log.error("Unable to locate feed definition with ident={}", feedIdent);
            return emptyList();
        }
        // fetch all pub-pending posts..
        List<StagingPost> pubPending = stagingPostDao.getPubPending(username, feedIdent);
        // fetch all currently published posts..
        List<StagingPost> pubCurrent = stagingPostDao.findPublishedByFeed(username, feedIdent);
        // ..`toPublish' is the union of both sets
        List<StagingPost> toPublish = new ArrayList<>(pubCurrent.size() + pubPending.size());
        toPublish.addAll(pubCurrent);
        toPublish.addAll(pubPending);
        // log startup
        log.info("Post publisher processing {} posts at {}", size(toPublish), getDateTimeInstance(MEDIUM, MEDIUM).format(new Date()));
        // invoke doPublish on ea. publisher and collect the results in a list
        Date pubDate = new Date();
        List<PubResult> publicationResults = publishers.stream()
                .map(publisher -> doPublish(publisher, feedDefinition, toPublish, pubDate))
                .collect(toList());
        // mark ea. post as pub complete (clear the status and set is_published to true)
        for (StagingPost n : pubPending) {
            this.stagingPostDao.markPubComplete(username, n.getId());
        }
        // fetch all de-pub-pending posts..
        List<StagingPost> depubPending = stagingPostDao.getDepubPending(username, feedIdent);
        // unmark ea. de-pub-pending post as pub complete (set the status to REVIEW and set is_published to false)
        for (StagingPost n : depubPending) {
            this.stagingPostDao.clearPubComplete(username, n.getId());
        }
        feedDefinitionDao.updateLastDeployed(username, feedIdent, pubDate);
        log.info("Published {} articles, de-published {} articles, at {}, result={}",
                size(pubPending), size(depubPending), pubDate, publicationResults);

        return publicationResults;
    }
    private PubResult doPublish(Publisher publisher, FeedDefinition feedDefinition, List<StagingPost> toPublish, Date pubDate) {
        try {
            return publisher.publishFeed(feedDefinition, toPublish, pubDate);
        } catch (Exception e) { // publisher *should* handle their own exceptions
            return PubResult.from(publisher.getPublisherId(), feedDefinition.getIdent(), feedDefinition.getTransportIdent(), singletonList(e), pubDate);
        }
    }

    @SuppressWarnings("unused")
    public List<FeedPreview> doPreview(String username, String feedIdent, PubFormat format) throws DataAccessException {
        FeedDefinition feedDefinition = this.feedDefinitionDao.findByFeedIdent(username, feedIdent);
        if (feedDefinition == null) {
            log.error("Unable to locate feed definition with ident={}", feedIdent);
            return emptyList();
        }
        List<StagingPost> pubPending = stagingPostDao.getPubPending(username, feedIdent);
        List<StagingPost> pubCurrent = stagingPostDao.findPublishedByFeed(username, feedIdent);
        List<StagingPost> toPublish = new ArrayList<>(pubCurrent.size() + pubPending.size());
        toPublish.addAll(pubCurrent);
        toPublish.addAll(pubPending);
        log.info("Post publisher previewing {} posts at {}", size(toPublish), getDateTimeInstance(MEDIUM, MEDIUM).format(new Date()));
        List<FeedPreview> feedPreviewArtifacts = publishers.stream()
                .filter(p -> p.supportsFormat(format))
                .flatMap(p -> {
                    try {
                        List<FeedPreview> feedPreviews = p.doPreview(username, toPublish, format);
                        return feedPreviews.stream();
                    } catch (Exception e) {
                        log.error("Something horrible happened previewing feedIdent={} due to: {}", feedIdent, e.getMessage(), e);
                    }
                    return null;
                })
                .collect(toList());
        log.info("Post publisher previewed {} articles at {}", size(pubPending), Instant.now());

        return feedPreviewArtifacts;
    }
}
