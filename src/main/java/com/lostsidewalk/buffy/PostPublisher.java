package com.lostsidewalk.buffy;

import com.lostsidewalk.buffy.model.RenderedFeedDao;
import com.lostsidewalk.buffy.publisher.FeedPreview;
import com.lostsidewalk.buffy.publisher.Publisher;
import com.lostsidewalk.buffy.publisher.Publisher.PubFormat;
import com.lostsidewalk.buffy.publisher.Publisher.PubResult;
import com.lostsidewalk.buffy.post.StagingPost;
import com.lostsidewalk.buffy.post.StagingPostDao;
import com.lostsidewalk.buffy.queue.QueueDefinition;
import com.lostsidewalk.buffy.queue.QueueDefinitionDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.time.Instant.now;
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
    QueueDefinitionDao queueDefinitionDao;

    @Autowired
    StagingPostDao stagingPostDao;

    @Autowired
    List<Publisher> publishers;

    @Autowired
    RenderedFeedDao renderedFeedDao;

    @PostConstruct
    public void postConstruct() {
        log.info("Post publisher constructed, publisherCt={}", size(publishers));
    }

    @SuppressWarnings("unused")
    public List<PubResult> publishFeed(String username, Long queueId) throws DataAccessException, DataUpdateException {
        QueueDefinition queueDefinition = this.queueDefinitionDao.findByQueueId(username, queueId);
        if (queueDefinition == null) {
            log.error("Unable to locate queue definition with Id={}", queueId);
            return emptyList();
        }
        // fetch all pub-pending posts..
        List<StagingPost> pubPending = stagingPostDao.getPubPending(username, queueId);
        // fetch all currently published posts..
        List<StagingPost> pubCurrent = stagingPostDao.findPublishedByQueue(username, queueId);
        // ..`toPublish' is the union of both sets
        List<StagingPost> toPublish = new ArrayList<>(pubCurrent.size() + pubPending.size());
        toPublish.addAll(pubCurrent);
        toPublish.addAll(pubPending);
        // log startup
        log.info("Post publisher processing {} posts at {}", size(toPublish), getDateTimeInstance(MEDIUM, MEDIUM).format(new Date()));
        // invoke doPublish on ea. publisher and collect the results in a list
        Date pubDate = new Date();
        List<PubResult> publicationResults = publishers.stream()
                .map(publisher -> doPublish(publisher, queueDefinition, toPublish, pubDate))
                .collect(toList());
        // mark ea. post as pub complete (clear the status and set is_published to true)
        for (StagingPost n : pubPending) {
            this.stagingPostDao.markPubComplete(username, n.getId());
        }
        // fetch all de-pub-pending posts..
        List<StagingPost> depubPending = stagingPostDao.getDepubPending(username, queueId);
        // unmark ea. de-pub-pending post as pub complete (set the status to REVIEW and set is_published to false)
        for (StagingPost n : depubPending) {
            this.stagingPostDao.clearPubComplete(username, n.getId());
        }
        queueDefinitionDao.updateLastDeployed(username, queueId, pubDate);
        log.info("Published {} articles, de-published {} articles, for queueId={} ({}) at {}, result={}",
                size(pubPending), size(depubPending), queueId, username, pubDate, publicationResults);

        return publicationResults;
    }

    @SuppressWarnings("unused")
    public void unpublishFeed(String username, Long queueId) throws DataAccessException, DataUpdateException {
        QueueDefinition queueDefinition = this.queueDefinitionDao.findByQueueId(username, queueId);
        if (queueDefinition == null) {
            log.error("Unable to locate queue definition with Id={}", queueId);
            return;
        }
        // fetch all pub-pending posts..
        List<StagingPost> pubPending = stagingPostDao.getPubPending(username, queueId);
        // fetch all currently published posts..
        List<StagingPost> pubCurrent = stagingPostDao.findPublishedByQueue(username, queueId);
        // ..`toUnpublish' is the union of both sets
        List<StagingPost> toUnpublish = new ArrayList<>(pubCurrent.size() + pubPending.size());
        toUnpublish.addAll(pubCurrent);
        toUnpublish.addAll(pubPending);
        // log startup
        Date unpubDate = new Date();
        log.info("Post publisher un-publishing {} posts at {}", size(toUnpublish), getDateTimeInstance(MEDIUM, MEDIUM).format(unpubDate));
        // de-publish the feed
        renderedFeedDao.deleteFeedAtTransportIdent(queueDefinition.getTransportIdent());
        // clear the pub complete flag from ea. post
        for (StagingPost n : toUnpublish) {
            this.stagingPostDao.clearPubComplete(username, n.getId());
        }
        // clear the last deployed timestamp on the queue (it's no longer deployed)
        queueDefinitionDao.clearLastDeployed(username, queueDefinition.getId());

        log.info("Un-published {} articles for queueId={} ({}) at {}", size(toUnpublish), queueId, username, unpubDate);
    }

    //
    //
    //

    private PubResult doPublish(Publisher publisher, QueueDefinition queueDefinition, List<StagingPost> toPublish, Date pubDate) {
        try {
            return publisher.publishFeed(queueDefinition, toPublish, pubDate);
        } catch (Exception e) { // publisher *should* handle their own exceptions
            return PubResult.from(publisher.getPublisherId(), singletonList(e), pubDate);
        }
    }

    @SuppressWarnings("unused")
    public List<FeedPreview> doPreview(String username, Long queueId, PubFormat format) throws DataAccessException {
        QueueDefinition queueDefinition = this.queueDefinitionDao.findByQueueId(username, queueId);
        if (queueDefinition == null) {
            log.error("Unable to locate queue definition with Id={}", queueId);
            return emptyList();
        }
        List<StagingPost> pubPending = stagingPostDao.getPubPending(username, queueId);
        List<StagingPost> pubCurrent = stagingPostDao.findPublishedByQueue(username, queueId);
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
                        log.error("Something horrible happened previewing queueId={} due to: {}", queueId, e.getMessage(), e);
                    }
                    return null;
                })
                .collect(toList());
        log.info("Post publisher previewed {} articles at {}", size(pubPending), now());

        return feedPreviewArtifacts;
    }
}
