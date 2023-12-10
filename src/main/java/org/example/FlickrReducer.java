package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FlickrReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> tagCount = new HashMap<>();

        for (Text value : values) {
            String tag = value.toString();
            tagCount.put(tag, tagCount.getOrDefault(tag, 0) + 1);
        }

        for (Map.Entry<String, Integer> entry : tagCount.entrySet()) {
            context.write(new Text(key.toString() + ": " + entry.getKey()), new IntWritable(entry.getValue()));
        }
    }
}