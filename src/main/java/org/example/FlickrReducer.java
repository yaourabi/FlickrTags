package org.example;

import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FlickrReducer implements Reducer<Text, Text, Text, IntWritable> {
    int K;

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> context, Reporter reporter) throws IOException {
        Map<String, Integer> tagCount = new HashMap<>();

        for (Iterator<Text> it = values; it.hasNext(); ) {
            Text value = it.next();
            String tag = value.toString();
            tagCount.put(tag, tagCount.getOrDefault(tag, 0) + 1);
        }
        MinMaxPriorityQueue<StringAndInt> maxPriorityQueue = MinMaxPriorityQueue.maximumSize(K).create();

        tagCount.forEach((tag, count) -> maxPriorityQueue.add(new StringAndInt(tag, count)));
        while (!maxPriorityQueue.isEmpty()){
            StringAndInt element = maxPriorityQueue.pollLast();
            context.collect(new Text(element.getTag()), new IntWritable(element.getNbOccurence()));
        }

        for (Map.Entry<String, Integer> entry : tagCount.entrySet()) {
            context.collect(new Text(key.toString() + ": " + entry.getKey()), new IntWritable(entry.getValue()));
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }

   /* @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> tagCount = new HashMap<>();

        for (Text value : values) {
            String tag = value.toString();
            tagCount.put(tag, tagCount.getOrDefault(tag, 0) + 1);
        }
        MinMaxPriorityQueue<StringAndInt> maxPriorityQueue = MinMaxPriorityQueue.maximumSize(K).create();

        tagCount.forEach((tag, count) -> maxPriorityQueue.add(new StringAndInt(tag, count)));
        while (!maxPriorityQueue.isEmpty()){
            StringAndInt element = maxPriorityQueue.pollLast();
            context.write(new Text(element.getTag()), new IntWritable(element.getNbOccurence()));
        }

        for (Map.Entry<String, Integer> entry : tagCount.entrySet()) {
            context.write(new Text(key.toString() + ": " + entry.getKey()), new IntWritable(entry.getValue()));
        }
    }*/
}