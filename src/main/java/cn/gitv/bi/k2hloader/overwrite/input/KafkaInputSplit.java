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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KafkaInputSplit extends InputSplit implements Writable {

    private String brokerId;
    private String broker;
    private int partition;
    private String topic;
    private long startOffset;

    // Needed for reflection instantiation (Required because we are implementing the Writable interface)
    public KafkaInputSplit() {
    }

    public KafkaInputSplit(int brokerId, String broker, String topic, int partition, long startOffset) {
        this.brokerId = String.valueOf(brokerId);
        this.broker = broker;
        this.partition = partition;
        this.topic = topic;
        this.startOffset = startOffset;
    }


    /*用来获取InputSplit的大小,以支持对InputSplits进行排序*/
    @Override
    public long getLength() throws IOException, InterruptedException {
        return Long.MAX_VALUE;
    }

    /*用来获取存储分片的位置列表*/
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{broker};
    }

    public String getBrokerId() {
        return brokerId;
    }

    /**
     * @return broker-id:host:port
     */
    public String getBroker() {
        return broker;
    }

    public String getBrokerHost() {
        String[] hostPort = broker.split(":");
        return hostPort[0];
    }

    public int getBrokerPort() {
        String[] hostPort = broker.split(":");
        return Integer.valueOf(hostPort[1]);
    }

    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    public long getStartOffset() {
        return startOffset;
    }

    @Override
    public String toString() {
        return String.format("Topic: %s Partition: %s Start-Offset: %s ",
                getTopic(), String.valueOf(getPartition()), String.valueOf(getStartOffset()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaInputSplit)) {
            return false;
        }

        KafkaInputSplit that = (KafkaInputSplit) o;

        if (startOffset != that.startOffset) {
            return false;
        } else if (partition != that.partition) {
            return false;
        } else if (!topic.equals(that.topic)) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + (int) startOffset;
        result = 31 * result + partition;
        return result;
    }


    /*反序列化*/
    public void readFields(DataInput in) throws IOException {
        brokerId = Text.readString(in);
        broker = Text.readString(in);
        topic = Text.readString(in);
        partition = in.readInt();
        startOffset = in.readLong();
    }

    /*序列化*/
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, brokerId);
        Text.writeString(out, broker);
        Text.writeString(out, topic);
        out.writeInt(partition);
        out.writeLong(startOffset);
    }
}