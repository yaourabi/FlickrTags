package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.net.URLDecoder;
import java.util.Arrays;

public class FlickrMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {

    private boolean isValidField(String field) {
        return field != null && !field.isEmpty();
    }

    private Country getCountry(String longitude, String latitude) {
        if (isValidField(longitude) && isValidField(latitude)) {
            return Country.getCountryAt(Double.parseDouble(latitude), Double.parseDouble(longitude));
        }
        return null;
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] records = value.toString().split("\t");
        String longitude = records[10];
        String latitude	 = records[11];

        Country country = getCountry(longitude, latitude);

        if (country != null) {
            String tags = URLDecoder.decode(records[8],"UTF-8") + "," + URLDecoder.decode(records[9],"UTF-8");

            for (String tag : tags.split(","))
                context.write(new Text(country.toString()), new StringAndInt(tag));
        }
    }

}