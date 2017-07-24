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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MsgMetaKeyWritable implements Writable {

    private Long timestamp;
    private KafkaInputSplit split;
    private long offset;

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }


    public MsgMetaKeyWritable() {
    }

    public MsgMetaKeyWritable(KafkaInputSplit split, long offset) {
        this.split = split;
        this.offset = offset;
        this.timestamp = 0L;
    }

    public MsgMetaKeyWritable(MsgMetaKeyWritable copyOf, Long timestamp) {
        this.split = copyOf.getSplit();
        this.offset = copyOf.getOffset();
        this.timestamp = timestamp;
    }

    public long getOffset() {
        return offset;
    }

    public KafkaInputSplit getSplit() {
        return split;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        split.write(out);
        WritableUtils.writeVLong(out, getOffset());
        WritableUtils.writeVLong(out, getTimestamp());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        split = new KafkaInputSplit();
        split.readFields(in);
        offset = WritableUtils.readVLong(in);
        timestamp = WritableUtils.readVLong(in);
    }

    @Override
    public String toString() {
        return split.toString() + String.format("Offset: %s", String.valueOf(getOffset()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MsgMetaKeyWritable)) {
            return false;
        }

        MsgMetaKeyWritable that = (MsgMetaKeyWritable) o;

        if (!split.equals(that.split)) {
            return false;
        } else if (offset != that.offset) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = split.hashCode();
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }

    /**
     * @return Long value in millisecons UTC or null if no timestamp is
     * associated with the message metadata
     */
    public Long getTimestamp() {
        return timestamp;
    }
}
