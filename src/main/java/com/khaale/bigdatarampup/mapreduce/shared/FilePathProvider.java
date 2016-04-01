package com.khaale.bigdatarampup.mapreduce.shared;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

/**
 * Created by Aleksander_Khanteev on 3/29/2016.
 */
public interface FilePathProvider {

    String LocalKey = "local.user.profile.tags";

    Path getUserTagDictionaryPath(Mapper.Context context);
}

