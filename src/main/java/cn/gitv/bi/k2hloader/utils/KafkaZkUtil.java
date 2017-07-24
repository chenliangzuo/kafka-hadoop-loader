package cn.gitv.bi.k2hloader.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Kang on 2017/7/23.
 */
public class KafkaZkUtil implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaZkUtil.class);
    private final CuratorFramework zkClient;
    public static final String CONSUMERS_PATH = "/consumers";
    public static final String BROKER_IDS_PATH = "/brokers/ids";
    public static final String BROKER_TOPICS_PATH = "/brokers/topics";
    public static final String PARTITIONS = "partitions";

    public KafkaZkUtil(String zkConnectString, int sessionTimeout, int connectTimeout) {
        zkClient = CuratorTools.getInstance(zkConnectString, sessionTimeout, connectTimeout).zkClient;
    }

//    public static void main(String args[]) throws Exception {
//        KafkaZkUtil kafkaZkUtil = new KafkaZkUtil("slave1:2181,slave2:2181,slave3:2181/kafka_2.10", 10000, 10000);
////        kafkaZkUtil.commitLastConsumedOffset("test", "test", 0, 245);
////        System.out.println(kafkaZkUtil.getLastConsumedOffset("test", "test", 0));
////        System.out.println(kafkaZkUtil.getPartitionLeaders("test"));
////        System.out.println(kafkaZkUtil.getBrokerInfo(110));
//        System.out.println(kafkaZkUtil.getBrokerName(110));
//    }

    /**
     * 返回子目录的集合,路径不正确返回空集合
     */
    private List<String> getChildrenParentMayNotExist(String path) {
        try {
            return zkClient.getChildren().forPath(path);
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }


    /**
     * @return 拼接zk中存取偏移量的path
     */
    private String getOffsetsPath(String group, String topic, int partition) {
        return CONSUMERS_PATH + "/" + group + "/offsets/" + topic + "/" + partition;
    }

    /**
     * 消费偏移量记录
     * 存在路径kafka_2.10/consumers/test/offsets/test/0 = 245,不存在创建
     */
    void commitLastConsumedOffset(String group, String topic, int partition, long offset) {
        String path = getOffsetsPath(group, topic, partition);
        LOG.info("OFFSET COMMIT " + path + " = " + offset);
        try {
            if (zkClient.checkExists().forPath(path) == null) {
                zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
            }
            zkClient.setData().forPath(path, (offset + "").getBytes());
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    /**
     * @return 返回消费者最后的偏移量
     */
    long getLastConsumedOffset(String group, String topic, int partition) {
        try {
            //获取offset路径znode
            String znode = getOffsetsPath(group, topic, partition);
            if (zkClient.checkExists().forPath(znode) == null) {
                zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(znode);
                zkClient.setData().forPath(znode, "0".getBytes());
            }
            //获取znode中内容offset
            String offset = new String(zkClient.getData().forPath(znode), "utf-8");
            if (offset == null) {
                return -1L;
            }
            return Long.valueOf(offset);
        } catch (Exception e) {
            LOG.info("", e);
        }
        return -1L;
    }

    /**
     * 获取所有分区的leader集合
     */
    public Map<Integer, Integer> getPartitionLeaders(String topic) {
        Map<Integer, Integer> partitionLeaders = new HashMap<>();
        try {
            String topicPartitionsPath = BROKER_TOPICS_PATH + "/" + topic + "/" + PARTITIONS;
            //获取/brokers/topics/xx(topic)/partitions的子目录
            List<String> partitions = getChildrenParentMayNotExist(topicPartitionsPath);
            for (String partition : partitions) {
                String data = new String(zkClient.getData().forPath(topicPartitionsPath + "/" + partition + "/state"), "utf-8");
                //解析zk中获取的json信息
                Map<String, Object> map = JSONObject.parseObject(data, new TypeReference<Map<String, Object>>() {
                });
                if (map.containsKey("leader"))
                    partitionLeaders.put(Integer.valueOf(partition), Integer.valueOf(map.get("leader").toString()));
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
        return partitionLeaders;
    }

    /**
     * 获取slave2:2181/kafka_2.10/brokers/ids/110下的data
     */
    public String getBrokerInfo(int brokerId) {
        String data = "";
        try {
            data = new String(zkClient.getData().forPath(BROKER_IDS_PATH + "/" + brokerId), "utf-8");
        } catch (Exception e) {
            LOG.error("", e);
        }
        return data;
    }

    /**
     * 解析getBrokerInfo()拿到的json:data 获取host:port
     */
    public String getBrokerName(int brokerId) {
        Map<String, Object> map = JSONObject.parseObject(getBrokerInfo(brokerId), new TypeReference<Map<String, Object>>() {
        });
        return map.get("host") + ":" + map.get("port");
    }

    @Override
    public void close() throws IOException {
        if (zkClient != null) {
            if (zkClient.getState() != CuratorFrameworkState.STOPPED)
                zkClient.close();
        }
    }
}

