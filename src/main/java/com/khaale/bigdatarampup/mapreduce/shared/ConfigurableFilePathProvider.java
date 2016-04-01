package com.khaale.bigdatarampup.mapreduce.shared;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import java.nio.file.Path;

public class ConfigurableFilePathProvider implements FilePathProvider {

    private final FilePathProvider localImpl;
    private final FilePathProvider distributedImpl;

    public ConfigurableFilePathProvider() {

       localImpl = new LocalFilePathProvider();
       distributedImpl = new DistributedCacheFilePathProvider();
    }

    @Override
    public Path getUserTagDictionaryPath(Mapper.Context context) {

        Configuration conf = context.getConfiguration();
        String localFilePath = conf.get(LocalKey);

        if (localFilePath != null) {
            return localImpl.getUserTagDictionaryPath(context);
        }
        else {
            return distributedImpl.getUserTagDictionaryPath(context);
        }
    }
}
