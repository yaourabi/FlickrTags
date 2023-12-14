package org.example;

import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public  class FlickrReducer extends Reducer<Text, StringAndInt, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> frequenceTags = new HashMap<>();

        for (StringAndInt value : values) {
            frequenceTags.put(value.tag, frequenceTags.getOrDefault(value.tag, 0) + value.occurence);
        }

        int numberOfTags = context.getConfiguration()
                .getInt("K", 1);

        MinMaxPriorityQueue<StringAndInt> topTags = MinMaxPriorityQueue.maximumSize(numberOfTags)
                .create();


        frequenceTags.forEach((tag, occurrence) -> topTags.add(new StringAndInt(tag, occurrence)));


        for (StringAndInt stringAndInt : topTags)
            context.write(key, new Text(stringAndInt.toString()));
    }

}