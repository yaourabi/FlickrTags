package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.net.URLDecoder;

public class FlickrMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        String[] parts = value.toString().split("\t");

        try {

            double latitude = Double.parseDouble(parts[11]);
            double longitude = Double.parseDouble(parts[10]);
            Country country = Country.getCountryAt(latitude, longitude);

            if (country != null) {
                String tags = URLDecoder.decode(parts[8], "UTF-8");
                for (String tag : tags.split(",")) {
                    context.write(new Text(country.toString()), new Text(tag));
                }
            }
        } catch (Exception e) {
            Counter counter = context.getCounter("FlickrPhotoMapper", "Malformatted Lines");
            counter.increment(1);
        }
    }
}