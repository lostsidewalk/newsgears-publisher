package com.lostsidewalk.buffy;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.collections4.CollectionUtils.size;


@Slf4j
@Component
public class PostPublisher {

    @Autowired
    StagingPostDao stagingPostDao;

    @Autowired
    List<Publisher> publishers;

    @PostConstruct
    public void postConstruct() {
        log.info("Post publisher constructed, publisherCt={}", CollectionUtils.size(publishers));
    }

    @Scheduled(fixedDelay = 1L, timeUnit = TimeUnit.DAYS)
    public void doPublish() {
        doPublish(null);
    }

    public void doPublish(String tag) {
        log.info("Post publisher process starting at {}", FastDateFormat.getDateTimeInstance(FastDateFormat.MEDIUM, FastDateFormat.MEDIUM).format(new Date()));
        List<StagingPost> pubPending = stagingPostDao.getPubPending(tag);
        if (isNotEmpty(pubPending)) {
            publishers.forEach(publisher -> publisher.doPublish(pubPending));
            pubPending.forEach(p -> this.stagingPostDao.markPubComplete(p.getId()));
        }
        log.info("Post publisher processed {} articles at {}", size(pubPending), Instant.now());
    }
}
