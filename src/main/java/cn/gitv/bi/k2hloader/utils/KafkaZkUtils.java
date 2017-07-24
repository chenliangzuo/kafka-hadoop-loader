///*
// * Copyright 2014 Michal Harish, michal.harish@gmail.com
// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package cn.gitv.bi.k2hloader.utils;
//
//import org.I0Itec.zkclient.ZkClient;
//import org.I0Itec.zkclient.exception.ZkMarshallingError;
//import org.I0Itec.zkclient.exception.ZkNoNodeException;
//import org.I0Itec.zkclient.serialize.ZkSerializer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.Closeable;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class KafkaZkUtils implements Closeable {
//
//    private static final Logger log = LoggerFactory.getLogger(KafkaZkUtils.class);
//    public static final String CONSUMERS_PATH = "/consumers";
//    public static final String BROKER_IDS_PATH = "/brokers/ids";
//    public static final String BROKER_TOPICS_PATH = "/brokers/topics";
//    public static final String PARTITIONS = "partitions";
//    private JacksonParseUtils jacksonParseUtils;
//
//    private final ZkClient client;
//
//    /**
//     * @param zkConnectString zk的连接host
//     * @param sessionTimeout  session的超时设置
//     * @param connectTimeout  连接超时
//     */
//    public KafkaZkUtils(String zkConnectString, int sessionTimeout, int connectTimeout) {
//        client = new ZkClient(zkConnectString, sessionTimeout, connectTimeout, new StringSerializer());
//
//        jacksonParseUtils = new JacksonParseUtils();
//        log.info("Connected zk");
//    }
//
//
//    /**
//     * @param brokerId kafka broker
//     * @return 返回broker的name
//     */
//    public String getBrokerName(int brokerId) {
//        Map<String, Object> map = jacksonParseUtils.parseJsonAsMap(getBrokerInfo(brokerId));
//        return map.get("host") + ":" + map.get("port");
//    }
//
//    /**
//     * @param brokerId kafka broker
//     * @return 返回broker的信息
//     */
//    public String getBrokerInfo(int brokerId) {
//        return client.readData(BROKER_IDS_PATH + "/" + brokerId);
//    }
//
//    /**
//     * Map of PartitionId : BrokerId where by BrokerId is the leader
//     *
//     * @param topic kafka topic
//     * @return 分区 对 分区leader
//     */
//    public Map<Integer, Integer> getPartitionLeaders(String topic) {
//        Map<Integer, Integer> partitionLeaders = new HashMap<>();
//        String topicPartitionsPath = BROKER_TOPICS_PATH + "/" + topic + "/" + PARTITIONS;
//        //获取/brokers/topics/xx(topic)/partitions的子目录
//        List<String> partitions = getChildrenParentMayNotExist(topicPartitionsPath);
//        for (String partition : partitions) {
//            String data = client.readData(topicPartitionsPath + "/" + partition + "/state");
//            System.out.println(data);
//            //解析zk中获取的json信息
//            Map<String, Object> map = jacksonParseUtils.parseJsonAsMap(data);
//            if (map.containsKey("leader"))
//                partitionLeaders.put(Integer.valueOf(partition), Integer.valueOf(map.get("leader").toString()));
//        }
//        return partitionLeaders;
//    }
//
//
//    /**
//     * @param group     所属组
//     * @param topic     topic
//     * @param partition partition值
//     * @return 返回消费者最后的偏移量
//     */
//    long getLastConsumedOffset(String group, String topic, int partition) {
//        //获取offset路径znode
//        String znode = getOffsetsPath(group, topic, partition);
//        //获取znode中内容offset
//        String offset = client.readData(znode, true);
//        if (offset == null) {
//            return -1L;
//        }
//        return Long.valueOf(offset);
//    }
//
//    /**
//     * @return 拼接zk中存取偏移量的path
//     */
//    private String getOffsetsPath(String group, String topic, int partition) {
//        return CONSUMERS_PATH + "/" + group + "/offsets/" + topic + "/" + partition;
//    }
//
//    /**
//     * @param offset 偏移量
//     *               提交消费后的最后offset
//     */
//    void commitLastConsumedOffset(String group, String topic, int partition, long offset) {
//        String path = getOffsetsPath(group, topic, partition);
//        log.info("OFFSET COMMIT " + path + " = " + offset);
//        if (!client.exists(path)) {
//            client.createPersistent(path, true);
//        }
//        //TODO use versioned zk.writeData in case antoher instance has advanced the offset
//        client.writeData(path, offset);
//    }
//
//    /**
//     * @param path 指定路径
//     * @return 返回子目录的集合
//     */
//    private List<String> getChildrenParentMayNotExist(String path) {
//        try {
//            return client.getChildren(path);
//        } catch (ZkNoNodeException e) {
//            return new ArrayList<>();
//        }
//    }
//
//    @Override
//    public void close() throws java.io.IOException {
//        if (client != null) {
//            client.close();
//        }
//    }
//
//    private static class StringSerializer implements ZkSerializer {
//
//        public Object deserialize(byte[] data) throws ZkMarshallingError {
//            if (data == null)
//                return null;
//            return new String(data);
//        }
//
//        public byte[] serialize(Object data) throws ZkMarshallingError {
//            return data.toString().getBytes();
//        }
//    }
//
//
//}
