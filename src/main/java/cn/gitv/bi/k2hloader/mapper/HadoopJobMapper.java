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

package cn.gitv.bi.k2hloader.mapper;

import cn.gitv.bi.k2hloader.inf.TimestampExtractor;
import cn.gitv.bi.k2hloader.overwrite.input.MsgMetaKeyWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HadoopJobMapper extends Mapper<MsgMetaKeyWritable, Text, MsgMetaKeyWritable, Text> {
    static Logger LOG = LoggerFactory.getLogger(HadoopJobMapper.class);
    private static final String CONFIG_TIMESTAMP_EXTRACTOR_CLASS = "mapper.timestamp.extractor.class";
    private TimestampExtractor extractor;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        try {
            Class<?> extractorClass = conf.getClass(CONFIG_TIMESTAMP_EXTRACTOR_CLASS, null);
            if (extractorClass != null) {
                extractor = extractorClass.asSubclass(TimestampExtractor.class).newInstance();
                LOG.info("Using timestamp extractor " + extractor);
            }

        } catch (Exception e) {
            throw new IOException(e);
        }
        super.setup(context);
    }

    @Override
    public void map(MsgMetaKeyWritable key, Text value, Context context) throws IOException {
        try {
            if (key != null) {
                MsgMetaKeyWritable outputKey = key;
                if (extractor != null) {
                    Long timestamp = extractor.extract(key, value);
                    outputKey = new MsgMetaKeyWritable(key, timestamp);
                } else {
                    outputKey = new MsgMetaKeyWritable(key, System.currentTimeMillis());
                }
                context.write(outputKey, value);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            throw e;
        }
    }

}