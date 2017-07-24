package cn.gitv.bi.k2hloader.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * 使用zk或者hdfs存储偏移量
 */
public class CheckpointManager {
    private static final String CONFIG_KAFKA_GROUP_ID = "checkpoints.zookeeper.group.id";
    private static final String CONFIG_CHECKPOINTS_ZOOKEEPER = "checkpoint.use.zookeeper";
    private final FileSystem fs;
    private final Boolean useZkCheckpoints;
    private final KafkaZkUtil zkUtils;
    private final Configuration conf;
    private final Path hdfsCheckpointDir;

    public static void configureUseZooKeeper(Configuration conf, String kafkaGroupId) {
        conf.setBoolean(CONFIG_CHECKPOINTS_ZOOKEEPER, true);
        conf.set(CONFIG_KAFKA_GROUP_ID, kafkaGroupId);
    }

    public CheckpointManager(Configuration conf, KafkaZkUtil zkUtil) throws IOException {
        fs = FileSystem.get(conf);
        this.conf = conf;
        this.zkUtils = zkUtil;
        useZkCheckpoints = conf.getBoolean(CONFIG_CHECKPOINTS_ZOOKEEPER, false);
        hdfsCheckpointDir = new Path(conf.get("mapreduce.output.fileoutputformat.outputdir"), "_OFFSETS");
    }

    public long getNextOffsetToConsume(String topic, int partition) throws IOException {
        if (useZkCheckpoints) {
            String consumerGroup = conf.get(CONFIG_KAFKA_GROUP_ID);
            return zkUtils.getLastConsumedOffset(consumerGroup, topic, partition) + 1;
        } else {
            long nextOffsetToConsume = 0L;
            Path comittedCheckpoint = new Path(hdfsCheckpointDir, topic + "-" + partition);
            if (fs.exists(comittedCheckpoint)) {
                try (FSDataInputStream in = fs.open(comittedCheckpoint)) {
                    in.readLong();// prev cn.bi.gitv.hip.parquetdemo.start offset
                    nextOffsetToConsume = in.readLong() + 1;
                }
            }
            Path pendingCheckpoint = new Path(hdfsCheckpointDir, "_" + topic + "-" + partition);
            if (fs.exists(pendingCheckpoint)) {
                //TODO #11 locking either crashed job or another instance
                fs.delete(pendingCheckpoint, true);
            }
            try (FSDataOutputStream out = fs.create(pendingCheckpoint)) {
                out.writeLong(nextOffsetToConsume);
            }
            ///
            //
            return nextOffsetToConsume;
        }
    }

    public void commitOffsets(String topic, int partition, long lastConsumedOffset) throws IOException {
        if (useZkCheckpoints) {
            String group = conf.get(CONFIG_KAFKA_GROUP_ID);
            zkUtils.commitLastConsumedOffset(group, topic, partition, lastConsumedOffset);
        } else {
            Path pendingCheckpoint = new Path(hdfsCheckpointDir, "_" + topic + "-" + partition);
            Path comittedCheckpoint = new Path(hdfsCheckpointDir, topic + "-" + partition);

            try (FSDataOutputStream out = fs.append(pendingCheckpoint)) {
                out.writeLong(lastConsumedOffset);
            }
            fs.rename(pendingCheckpoint, comittedCheckpoint);
        }
    }
}
