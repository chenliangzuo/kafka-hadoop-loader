package cn.gitv.bi.k2hloader.inf;

import cn.gitv.bi.k2hloader.mapper.HadoopJobMapper;
import cn.gitv.bi.k2hloader.overwrite.input.MsgMetaKeyWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Kang on 2017/7/24.
 */
public class TimeStampExample implements TimestampExtractor {
    static Logger LOG = LoggerFactory.getLogger(TimeStampExample.class);

    @Override
    public Long extract(MsgMetaKeyWritable key, Text value) throws IOException {
        String line = value.toString();
        String time = line.split("\t")[0];
        long timeStamp = 0;
        try {
            timeStamp = Long.parseLong(time);
        } catch (Exception e) {
            LOG.error("", e);
        }
        return timeStamp;
    }
}
