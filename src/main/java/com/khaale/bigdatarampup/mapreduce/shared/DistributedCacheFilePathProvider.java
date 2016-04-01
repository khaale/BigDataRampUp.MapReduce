package com.khaale.bigdatarampup.mapreduce.shared;

import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

public class DistributedCacheFilePathProvider implements FilePathProvider {

    private final static Logger logger = LoggerFactory.getLogger(DistributedCacheFilePathProvider.class);

    @Override
    public Path getUserTagDictionaryPath(Mapper.Context context) {

        final URI[] cacheArchives;
        try {
            cacheArchives = context.getCacheFiles();


            for (URI uri : cacheArchives){
                logger.info("Cache file: {}", uri.toString());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (cacheArchives == null || cacheArchives.length == 0) {
            throw new RuntimeException("User tags dictionary file must be provided!");
        }

        return new File("./user.profile.tags.us.txt").toPath();
    }
}

