package cn.gitv.bi.k2hloader.overwrite.output;

import cn.gitv.bi.k2hloader.overwrite.input.MsgMetaKeyWritable;
import cn.gitv.bi.k2hloader.utils.StringHandle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Created by Kang on 2017/7/24.
 */
public class MultiOutRecordWriter extends RecordWriter<MsgMetaKeyWritable, Text> {
    final private static Logger LOG = LoggerFactory.getLogger(MultiOutRecordWriter.class);
    TreeMap<String, RecordWriter<Void, Text>> recordWriters = new TreeMap<>();//内存中维护文件io的写流操作
    private static final String CONFIG_PATH_FORMAT = "multioutput.path.format";
    private Configuration conf;
    private String pathFormat;
    private SimpleDateFormat timeFormat;
    private DecimalFormat offsetFormat;
    private Path basePath;
    private String extension;
    private CompressionCodec codec;
    private boolean hasTS = false;


    public MultiOutRecordWriter(Configuration conf, Path basePath, CompressionCodec codec, String extension) {
        this.conf = conf;
        this.codec = codec;
        this.extension = extension;
        this.pathFormat = conf.get(CONFIG_PATH_FORMAT, "'{T}/'yyyy-MM-dd'/'");
        timeFormat = new SimpleDateFormat(this.pathFormat);
        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        //
        this.offsetFormat = new DecimalFormat("0000000000000000000");
        //
        this.hasTS = false;
        this.basePath = basePath;
    }

    /**
     * 根据MsgMetaKeyWritable key去构建一个具体路径下的具体文件名字
     */
    private String buildSuffixPath(MsgMetaKeyWritable key) {
        if (hasTS && key.getTimestamp() == null) {
            //extractor didn't wish to throw exception so skipping this record
            LOG.warn("i am coming return [hasTS] && [key.getTimestamp()]");
            return "";
        }
        String topic = key.getSplit().getTopic();
        String partition = String.valueOf(key.getSplit().getPartition());//just use in filename
        String suffixPath;
        if (hasTS || key.getTimestamp() != null) {
            suffixPath = timeFormat.format(key.getTimestamp());
        } else {
            suffixPath = pathFormat.replaceAll("'", "");
        }
        suffixPath = suffixPath.replace("{T}", topic);
        String fileName = topic + "-" + partition + "-" + offsetFormat.format(key.getSplit().getStartOffset());
        suffixPath = StringHandle.fullPath(suffixPath, fileName);
        suffixPath += extension;
        return suffixPath;
    }

    @Override
    public void write(MsgMetaKeyWritable key, Text value) throws IOException, InterruptedException {
        String suffixPath = buildSuffixPath(key);
        RecordWriter<Void, Text> recordWriter = this.recordWriters.get(suffixPath);
        try {
            if (recordWriter == null) {
                Path file = new Path(basePath, suffixPath);
                FileSystem fs = file.getFileSystem(conf);
                FSDataOutputStream fileOut = fs.create(file, false);
                if (codec != null) {
                    //使用压缩
                    recordWriter = new LineRecordTrueWriter(new DataOutputStream(codec.createOutputStream(fileOut)));
                } else {
                    //不使用压缩
                    recordWriter = new LineRecordTrueWriter(fileOut);
                }
                this.recordWriters.put(suffixPath, recordWriter);
            }
            recordWriter.write(null, value);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        /*循环关闭所有recordWriter,并清空recordWriters集合*/
        Iterator<String> keys = this.recordWriters.keySet().iterator();
        while (keys.hasNext()) {
            RecordWriter<Void, Text> rw = this.recordWriters.get(keys.next());
            rw.close(context);
        }
        this.recordWriters.clear();
    }


    protected static class LineRecordTrueWriter extends RecordWriter<Void, Text> {
        private static final byte[] newline = String.format("%n").getBytes();
        protected DataOutputStream out;

        public LineRecordTrueWriter(DataOutputStream out) {
            this.out = out;
        }

        public synchronized void write(Void key, Text value)
                throws IOException {
            out.write(value.getBytes(), 0, value.getLength());
            out.write(newline);
        }

        public synchronized void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }
}
