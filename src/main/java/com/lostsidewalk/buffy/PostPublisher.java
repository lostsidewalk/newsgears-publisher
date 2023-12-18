package com.lostsidewalk.buffy;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.lostsidewalk.buffy.model.RenderedFeedDao;
import com.lostsidewalk.buffy.post.StagingPost;
import com.lostsidewalk.buffy.post.StagingPostDao;
import com.lostsidewalk.buffy.publisher.FeedPreview;
import com.lostsidewalk.buffy.publisher.Publisher;
import com.lostsidewalk.buffy.publisher.Publisher.PubFormat;
import com.lostsidewalk.buffy.publisher.Publisher.PubResult;
import com.lostsidewalk.buffy.queue.QueueDefinition;
import com.lostsidewalk.buffy.queue.QueueDefinitionDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;

import static com.lostsidewalk.buffy.post.StagingPost.PostPubStatus.DEPUB_PENDING;
import static com.lostsidewalk.buffy.post.StagingPost.PostPubStatus.PUB_PENDING;
import static java.time.Instant.now;
import static java.util.Collections.*;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.collections4.CollectionUtils.size;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.time.FastDateFormat.MEDIUM;
import static org.apache.commons.lang3.time.FastDateFormat.getDateTimeInstance;


/**
 * The PostPublisher class is responsible for publishing and unpublishing posts from queues.
 * It coordinates the publishing process by invoking registered publishers and manages post statuses.
 */
@Slf4j
@Component
public class PostPublisher {

    @Autowired
    private QueueDefinitionDao queueDefinitionDao;

    @Autowired
    private StagingPostDao stagingPostDao;

    @Autowired
    private List<Publisher> publishers;

    @Autowired
    private RenderedFeedDao renderedFeedDao;

    @Autowired
    private LockDao lockDao;

    /**
     * Default constructor; initializes the object.
     */
    PostPublisher() {
    }

    /**
     * Initializes the PostPublisher component after construction and logs the number of publishers available.
     */
    @PostConstruct
    protected final void postConstruct() {
        log.info("Post publisher constructed, publisherCt={}", size(publishers));
    }

    /**
     * (Convenience method to re-publish a feed without adding or updating any staging posts.)
     *
     * @param username      The username of the user.
     * @param queueId       The ID of the queue to publish.
     * @return A map containing publication results for each publisher.
     * @throws DataAccessException   If there is a data access issue.
     * @throws DataUpdateException   If there is an issue updating data.
     */
    @SuppressWarnings("unused")
    public final Map<String, PubResult> publishFeed(String username, Long queueId) throws DataAccessException, DataUpdateException {
        return publishFeed(username, queueId, emptyList());
    }

    /**
     * Publishes a feed for a given user and queue, updating the post statuses accordingly.
     *
     * @param username      The username of the user.
     * @param queueId       The ID of the queue to publish.
     * @param stagingPosts  A list of staging posts to publish.
     * @return A map containing publication results for each publisher.
     * @throws DataAccessException   If there is a data access issue.
     * @throws DataUpdateException   If there is an issue updating data.
     */
    @SuppressWarnings({"unused", "WeakerAccess"})
    public final Map<String, PubResult> publishFeed(String username, Long queueId, List<StagingPost> stagingPosts) throws DataAccessException, DataUpdateException {
        // (0) prerequisite: fetch the queue to publish
        QueueDefinition queueDefinition = queueDefinitionDao.findByQueueId(username, queueId);
        if (queueDefinition == null) {
            log.error("Unable to locate queue definition with Id={}", queueId);
            return emptyMap();
        }

        // (0.5) acquire a lock on the rendered feeds for this queue
        String lockKey = queueDefinition.getTransportIdent();
        String lockValue = randomUUID().toString();
        if (!lockDao.acquireLockWithRetry(lockKey, lockValue)) {
            log.error("Unable to acquire a lock on the rendered feeds for queue Id={}, lockKey={}, lockValue={}", queueId, lockKey, lockValue);
            return emptyMap();
        }

        // (1) build a stack of posts that we need to (possibly) publish
        stagingPosts.sort(Comparator.comparing(StagingPost::getCreated));
        @SuppressWarnings("UseOfObsoleteCollectionType") Stack<StagingPost> pubStack = new Stack<>(); // I like stacks
        stagingPosts.stream()
                .filter(status -> status.getPostPubStatus() == PUB_PENDING)
                .forEach(pubStack::push);

        // (2) compute the number of available slots for new posts (null is unlimited)
        JsonObject exportConfigObj = getExportConfigObj(queueDefinition);
        Integer maxPublished = exportConfigObj.has("maxPublished") ?
                exportConfigObj.get("maxPublished").getAsInt() :
                null; // (default max publishable)
        List<StagingPost> publishedCurrent = defaultIfNull(stagingPostDao.findPublishedByQueue(username, queueId), emptyList());
        Integer availableSlots = maxPublished == null ? null : (maxPublished - size(publishedCurrent));
        if (availableSlots != null && availableSlots < 0) {
            log.warn("Number of available slots ({}) in queue Id {} has been exceeded: {}.  De-publish existing posts in order to publish new posts.",
                    maxPublished, queueId, -1 * availableSlots);
        }

        // (3) map up currently published posts
        Map<Long, StagingPost> map = new HashMap<>(size(publishedCurrent));
        publishedCurrent.forEach(p -> map.put(p.getId(), p));

        // (4) add new posts where we have enough slots
        int pubCt = pubStack.size();
        //noinspection MethodCallInLoopCondition
        while (!pubStack.empty()) {
            StagingPost pubCandidate = pubStack.pop();
            boolean addNewPost = !map.containsKey(pubCandidate.getId()) && (availableSlots == null || availableSlots > 0);
            if (addNewPost) {
                map.put(pubCandidate.getId(), pubCandidate);
                if (availableSlots != null) {
                    availableSlots--;
                }
            }
        }

        // (5) build the final collection of posts to publish
        Collection<StagingPost> toPublish = map.values();

        // (6) log startup and invoke the publishers
        Date pubDate = new Date();
        log.info("Post publisher processing {} posts at {}", size(toPublish), getDateTimeInstance(MEDIUM, MEDIUM).format(pubDate));
        Map<String, PubResult> publicationResults = new HashMap<>(size(publishers) << 1);
        publishers.forEach(p -> publicationResults.putAll(doPublish(p, queueDefinition, new HashSet<>(toPublish), pubDate)));

        // (6.5) release the lock on the rendered feeds for this queue
        if (!lockDao.releaseLock(lockKey, lockValue)) {
            log.warn("Unable to release lock, lockKey={}, lockValue={}", lockKey, lockValue);
        }

        // (7) mark ea. published post as pub complete
        markPubComplete(username, toPublish);

        // (8) clear pub complete for ea. depublished post
        List<StagingPost> depubPending = clearPubComplete(username, stagingPosts);

        // (9) update queue last deployed timestamp
        if (isNotEmpty(toPublish) || isNotEmpty(depubPending)) {
            queueDefinitionDao.updateLastDeployed(username, queueId, pubDate);
        }

        // (10) log final results
        log.info("Published {} articles, de-published {} articles, for queueId={} ({}) at {}, result={}",
                size(stagingPosts), size(depubPending), queueId, username, pubDate, publicationResults);

        return publicationResults;
    }

    private static final Gson GSON = new Gson();

    private static JsonObject getExportConfigObj(QueueDefinition queueDefinition) {
        return ofNullable(queueDefinition.getExportConfig())
                .map(Object::toString)
                .map(s -> GSON.fromJson(s, JsonObject.class))
                .orElse(new JsonObject());
    }

    /**
     * Unpublishes a previously published feed for a given user and queue.
     *
     * @param username The username of the user.
     * @param queueId  The ID of the queue to unpublish.
     * @throws DataAccessException If there is a data access issue.
     * @throws DataUpdateException If there is an issue updating data.
     */
    @SuppressWarnings("unused")
    public final void unpublishFeed(String username, Long queueId) throws DataAccessException, DataUpdateException {
        QueueDefinition queueDefinition = queueDefinitionDao.findByQueueId(username, queueId);
        if (queueDefinition == null) {
            log.error("Unable to locate queue definition with Id={}", queueId);
            return;
        }
        // acquire lock
        String lockKey = queueDefinition.getTransportIdent();
        String lockValue = randomUUID().toString();
        if (!lockDao.acquireLockWithRetry(lockKey, lockValue)) {
            log.error("Unable to acquire a lock on the rendered feeds for queue Id={}, lockKey={}, lockValue={}", queueId, lockKey, lockValue);
            return;
        }
        // fetch all pub-pending posts..
        List<StagingPost> pubPending = stagingPostDao.getPubPending(username, queueId);
        // fetch all currently published posts..
        List<StagingPost> pubCurrent = stagingPostDao.findPublishedByQueue(username, queueId);
        // ..`toUnpublish' is the union of both sets
        Collection<StagingPost> toUnpublish = new ArrayList<>(pubCurrent.size() + pubPending.size());
        toUnpublish.addAll(pubCurrent);
        toUnpublish.addAll(pubPending);
        // log startup
        Date unpubDate = new Date();
        log.info("Post publisher un-publishing {} posts at {}", size(toUnpublish), getDateTimeInstance(MEDIUM, MEDIUM).format(unpubDate));
        // de-publish the feed
        renderedFeedDao.deleteFeedAtTransportIdent(queueDefinition.getTransportIdent());
        // release lock
        if (!lockDao.releaseLock(lockKey, lockValue)) {
            log.warn("Unable to release lock, lockKey={}, lockValue={}", lockKey, lockValue);
        }
        // clear the pub complete flag from ea. post
        for (StagingPost stagingPost : toUnpublish) {
            stagingPostDao.clearPubComplete(username, stagingPost.getId());
        }
        // clear the last deployed timestamp on the queue (it's no longer deployed)
        queueDefinitionDao.clearLastDeployed(username, queueDefinition.getId());

        log.info("Un-published {} articles for queueId={} ({}) at {}", size(toUnpublish), queueId, username, unpubDate);
    }

    //
    //
    //

    private static Map<String, PubResult> doPublish(Publisher publisher, QueueDefinition queueDefinition, Collection<StagingPost> toPublish, Date pubDate) {
        try {
            return publisher.publishFeed(queueDefinition, toPublish.stream().toList(), pubDate);
        } catch (RuntimeException e) {
            return Map.of(publisher.getPublisherId(), PubResult.from(null, null, singletonList(e), pubDate));
        }
    }

    private void markPubComplete(String username, Iterable<? extends StagingPost> toPublish) throws DataAccessException, DataUpdateException {
        for (StagingPost stagingPost : toPublish) {
            stagingPostDao.markPubComplete(username, stagingPost.getId());
        }
    }

    private List<StagingPost> clearPubComplete(String username, Collection<StagingPost> stagingPosts) throws DataAccessException, DataUpdateException {
        List<StagingPost> depubPending = stagingPosts.stream()
                .filter(status -> status.getPostPubStatus() == DEPUB_PENDING)
                .toList();
        for (StagingPost stagingPost : depubPending) {
            stagingPostDao.clearPubComplete(username, stagingPost.getId());
        }

        return depubPending;
    }

    /**
     * Generates feed previews for posts in a queue in the specified format.
     *
     * @param username The username of the user.
     * @param queueId  The ID of the queue to generate previews for.
     * @param format   The format of the feed previews.
     * @return A list of feed preview artifacts.
     * @throws DataAccessException If there is a data access issue.
     */
    @SuppressWarnings("unused")
    public final List<FeedPreview> doPreview(String username, Long queueId, PubFormat format) throws DataAccessException {
        QueueDefinition queueDefinition = queueDefinitionDao.findByQueueId(username, queueId);
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

    @Override
    public String toString() {
        return "PostPublisher{" +
                "queueDefinitionDao=" + queueDefinitionDao +
                ", stagingPostDao=" + stagingPostDao +
                ", publishers=" + publishers +
                ", renderedFeedDao=" + renderedFeedDao +
                ", lockDao=" + lockDao +
                '}';
    }
}
