package cn.gitv.bi.k2hloader.start;

import cn.gitv.bi.k2hloader.inf.TimeStampExample;
import cn.gitv.bi.k2hloader.inf.TimestampExtractor;
import cn.gitv.bi.k2hloader.mapper.HadoopJobMapper;
import cn.gitv.bi.k2hloader.overwrite.input.KafkaInputFormat;
import cn.gitv.bi.k2hloader.overwrite.output.MultiOutputFormat;
import cn.gitv.bi.k2hloader.utils.CheckpointManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartJob {
    private final static Logger LOG = LoggerFactory.getLogger(StartJob.class);
    private static final String CONFIG_TIMESTAMP_EXTRACTOR_CLASS = "mapper.timestamp.extractor.class";
    private static String zkConnection;
    private static String topic;
    private static String consumerGroup;
    private static String hdfsPath;
    private static String offset;
    private static String isCompress;

    public static void main(String args[]) throws Exception {
        if (args.length == 6) {
            zkConnection = args[0];
            topic = args[1];
            consumerGroup = args[2];
            hdfsPath = args[3];
            offset = args[4];
            isCompress = args[5];
        } else if (args.length == 4) {
            zkConnection = args[0];
            topic = args[1];
            consumerGroup = args[2];
            hdfsPath = args[3];
            offset = "earliest";
            isCompress = "off";
        } else {
            System.exit(0);
        }
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setClass(CONFIG_TIMESTAMP_EXTRACTOR_CLASS, TimeStampExample.class, TimestampExtractor.class);

        KafkaInputFormat.configureKafkaTopics(conf, topic);
        KafkaInputFormat.configureZkConnection(conf, zkConnection);
        CheckpointManager.configureUseZooKeeper(conf, consumerGroup);
        KafkaInputFormat.configureAutoOffsetReset(conf, offset);
        Job job = Job.getInstance(conf, "kafka.hadoop.loader");
        job.setJarByClass(StartJob.class);
        job.setInputFormatClass(KafkaInputFormat.class);
        job.setMapperClass(HadoopJobMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(MultiOutputFormat.class);
        job.setNumReduceTasks(0);
        MultiOutputFormat.setOutputPath(job, new Path(hdfsPath));
        MultiOutputFormat.configurePathFormat(conf, "'{T}/'yyyy-MM-dd'/'");
        MultiOutputFormat.setCompressOutput(job, isCompress.equals("on"));
        LOG.info("Output hdfs location: {}", hdfsPath);
        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }


}
