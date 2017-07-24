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

package cn.gitv.bi.k2hloader.overwrite.input;

import cn.gitv.bi.k2hloader.utils.CheckpointManager;
import cn.gitv.bi.k2hloader.utils.KafkaZkUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaInputFormat extends InputFormat<MsgMetaKeyWritable, Text> {

    public static final String CONFIG_ZK_CONNECT = "kafka.zookeeper.connect";
    public static final String CONFIG_ZK_SESSION_TIMEOUT_MS = "kafka.zookeeper.session.timeout.ms";
    public static final String CONFIG_ZK_CONNECT_TIMEOUT_MS = "kafka.zookeeper.connection.timeout.ms";
    public static final String CONFIG_KAFKA_TOPIC_LIST = "kafka.topics";

    /**
     * possible values: `earliest`, `latest` or `watermark`
     */
    static final String CONFIG_KAFKA_AUTOOFFSET_RESET = "kafka.watermark.reset";
    public static final String CONFIG_KAFKA_MESSAGE_MAX_BYTES = "kafka.fetch.message.max.bytes";
    public static final String CONFIG_KAFKA_SOCKET_TIMEOUT_MS = "kafka.socket.timeout.ms";
    public static final String CONFIG_KAFKA_RECEIVE_BUFFER_BYTES = "kafka.socket.receive.buffer.bytes";

    public static void configureKafkaTopics(Configuration conf, String comaSeparatedTopicNames) {
        conf.set(KafkaInputFormat.CONFIG_KAFKA_TOPIC_LIST, comaSeparatedTopicNames);
    }

    public static void configureZkConnection(Configuration conf, String zkConnectString) {
        conf.set(CONFIG_ZK_CONNECT, zkConnectString);
    }

    public static void configureZkTimeouts(Configuration conf, int sessionTimeoutMs, int connectTimeoutMs) {
        conf.setInt(CONFIG_ZK_SESSION_TIMEOUT_MS, sessionTimeoutMs);
        conf.setInt(CONFIG_ZK_CONNECT_TIMEOUT_MS, connectTimeoutMs);
    }

    public static void configureAutoOffsetReset(Configuration conf, String optionValue) {
        conf.set(CONFIG_KAFKA_AUTOOFFSET_RESET, optionValue);
    }

    /**
     * 这个由客户端调用来获得当前Job的所有分片
     *
     * @return 返回切片的集合
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        //一个topic的情况下,每个分区对应一个分片
        Configuration conf = context.getConfiguration();
        KafkaZkUtil zkUtils = new KafkaZkUtil(conf.get(KafkaInputFormat.CONFIG_ZK_CONNECT), conf.getInt(KafkaInputFormat.CONFIG_ZK_SESSION_TIMEOUT_MS, 10000), conf.getInt(KafkaInputFormat.CONFIG_ZK_CONNECT_TIMEOUT_MS, 10000));
        String[] inputTopics = conf.get(CONFIG_KAFKA_TOPIC_LIST).split(",");
        CheckpointManager checkpoints = new CheckpointManager(conf, zkUtils);
        List<InputSplit> splits = new ArrayList<InputSplit>();
        for (String topic : inputTopics) {
            Map<Integer, Integer> partitionLeaders = zkUtils.getPartitionLeaders(topic);
            for (int partition : partitionLeaders.keySet()) {
                int brokerId = partitionLeaders.get(partition);
                KafkaInputSplit split = new KafkaInputSplit(brokerId, zkUtils.getBrokerName(brokerId), topic, partition, checkpoints.getNextOffsetToConsume(topic, partition));
                splits.add(split);
            }
        }
        return splits;

    }

    @Override
    public RecordReader<MsgMetaKeyWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        return new KafkaInputRecordReader();
    }

}