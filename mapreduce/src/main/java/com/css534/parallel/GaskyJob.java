package com.css534.parallel;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Arrays;

public class GaskyJob {
    public static void main(String[] args) throws InterruptedException, IOException {

        JobConf conf = new JobConf(GaskyJob.class);

        conf.setJobName("Inverted Index");
        conf.setMapperClass(GaskyMapper.class);

        conf.setOutputKeyComparatorClass(RouteComparator.class);
        // conf.setCombinerClass(InvertedIndexReducer.class);
        conf.setReducerClass(GaskyReducer.class);

        conf.setMapOutputKeyClass(MapKeys.class);
        conf.setMapOutputValueClass(MapValue.class);


        conf.setJarByClass(GaskyJob.class);

        conf.setInputFormat(TextInputFormat.class); // record reader format and split procedure.
        conf.setOutputFormat(TextOutputFormat.class);

        System.out.println(Arrays.toString(args));
        FileInputFormat.addInputPath(conf , new Path(args[0])); // for the record redear in mapper
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient client = new JobClient();
        client.runJob(conf);
    }
}