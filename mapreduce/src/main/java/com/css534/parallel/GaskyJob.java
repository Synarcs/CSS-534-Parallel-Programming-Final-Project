package com.css534.parallel;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

import java.io.IOException;
import java.util.Arrays;

public class GaskyJob {

    private static  JobControl getJobController(){
        return new JobControl("Gasky Algorithm Computation");
    }

    public static void main(String[] args) throws InterruptedException, IOException {

        long time = System.currentTimeMillis();

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

        JobConf job2 = new JobConf();

        JobControl control  = getJobController();


//        Job findReducedDistancesJob = new Job(conf);
        client.runJob(conf);


        System.out.println("Elapsed Time :" + (System.currentTimeMillis() - time));
    }
}