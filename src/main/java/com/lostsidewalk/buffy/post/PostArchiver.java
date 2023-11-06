package com.lostsidewalk.buffy.post;

import com.lostsidewalk.buffy.DataUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Component responsible for purging archived posts, marking idle posts for archival,
 * and purging deleted queues and orphaned query metrics based on configured properties.
 */
@Slf4j
@Component
public class PostArchiver {

    @Autowired
    StagingPostDao stagingPostDao;

    /**
     * Default constructor; initializes the object.
     */
    PostArchiver() {
    }

    /**
     * Initializes the PostArchiver after construction.
     * Logs an informational message to indicate the archiver has been constructed.
     */
    @PostConstruct
    protected static void postConstruct() {
        log.info("Archiver constructed");
    }

    /**
     * Marks expired posts for archival.
     *
     * @return The staging posts marked for archival.
     * @throws DataUpdateException If there is an issue updating the data.
     */
    @SuppressWarnings("unused")
    public final List<StagingPost> markExpiredPostsForArchive() throws DataUpdateException {
        log.debug("Marking expired posts for archival");
        return stagingPostDao.markExpiredPostsForArchive();
    }

    @Override
    public final String toString() {
        return "PostArchiver{" +
                "stagingPostDao=" + stagingPostDao +
                '}';
    }
}
