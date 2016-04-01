package com.khaale.bigdatarampup.mapreduce.shared;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import java.nio.file.Path;
import java.nio.file.Paths;

public class LocalFilePathProvider implements FilePathProvider {

    public LocalFilePathProvider() {

    }

    @Override
    public Path getUserTagDictionaryPath(Mapper.Context context) {

        Configuration conf = context.getConfiguration();
        String localFilePath = conf.get(LocalKey);

        if (localFilePath != null) {
            return Paths.get(localFilePath);
        }
        else {
            throw new RuntimeException("Local path must be specified!");
        }
    }
}

