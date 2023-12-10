package org.example;

import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FlickrReducer extends Reducer<Text, Text, Text, IntWritable> {
    int K;

    @Override
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
    }
}