package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: FlickrTag <input path> <output path> <K>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("K", Integer.parseInt(args[2])); // Pass K to configuration

        Job job = Job.getInstance(conf, "Flickr Tag");
        job.setJarByClass(Main.class);
        job.setMapperClass(FlickrMapper.class);
        job.setReducerClass(FlickrReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}