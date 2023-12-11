package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

/*public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: FlickrTag <input path> <output path> <K>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("K", Integer.parseInt(args[2])); // Pass K to configuration

        JobConf jobConf = new JobConf(conf);
        jobConf.setJobName("Flickr Tag");

        jobConf.setMapperClass(FlickrMapper.class);
        jobConf.setReducerClass(FlickrReducer.class);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        Job job = new org.apache.hadoop.mapred.Job(jobConf);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
*/