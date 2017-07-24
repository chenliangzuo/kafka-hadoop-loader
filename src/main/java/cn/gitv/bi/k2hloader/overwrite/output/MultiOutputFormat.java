/*
 * Copyright 2014 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.gitv.bi.k2hloader.overwrite.output;

import cn.gitv.bi.k2hloader.overwrite.input.MsgMetaKeyWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MultiOutputFormat extends FileOutputFormat<MsgMetaKeyWritable, Text> {

    final private static Logger LOG = LoggerFactory.getLogger(MultiOutputFormat.class);

    private static final String CONFIG_PATH_FORMAT = "multioutput.path.format";

    /**
     * @param format relative path format, e.g:'{T}/'yyyy-MM-dd'/'
     */
    public static void configurePathFormat(Configuration conf, String format) {
        conf.set(CONFIG_PATH_FORMAT, format);
    }


    @Override
    public RecordWriter<MsgMetaKeyWritable, Text> getRecordWriter(TaskAttemptContext context) throws IOException {
        boolean isCompressed = getCompressOutput(context);
        Configuration conf = context.getConfiguration();
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        Path basePath = ((FileOutputCommitter) getOutputCommitter(context)).getWorkPath();
        return new MultiOutRecordWriter(conf, basePath, codec, extension);
    }


    @Override
    public void checkOutputSpecs(JobContext job) throws IOException {
        // Ensure that the output directory is set and not already there
        Path outDir = getOutputPath(job);
        if (outDir == null) {
            throw new InvalidJobConfException("Output directory not set.");
        }

        // get delegation token for outDir's file system
        TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[]{outDir}, job.getConfiguration()
        );
    }
}
