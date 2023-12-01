package com.css534.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

import com.css534.parallel.base.FacilityCombinerReducerBase;

import java.io.IOException;
import java.util.Arrays;

public class GaskyJob {

    private static  JobControl getJobController(){
        return new JobControl("Gasky Algorithm Computation");
    }

    private static Log logger = LogFactory.getLog(GaskyJob.class);


    public static void main(String[] args) throws InterruptedException, IOException {

        long time = System.currentTimeMillis();

        JobConf conf = new JobConf(GaskyJob.class);

        String favourableFacilitiesCount = args[3];
        String unFavourableFacilitiesCount = args[4];
        String includeDistance = args[5]; // true or false;

        String[] extraConfig = {favourableFacilitiesCount, unFavourableFacilitiesCount, includeDistance};

        conf.setJobName("Gasky Index");
        conf.setMapperClass(GaskyMapper.class);
        conf.setOutputKeyComparatorClass(RouteComparator.class);
        // conf.setCombinerClass(InvertedIndexReducer.class);
        conf.setReducerClass(GaskyReducer.class);
        conf.setMapOutputKeyClass(MapKeys.class);
        conf.setMapOutputValueClass(MapValue.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setJarByClass(GaskyJob.class);
        conf.setInputFormat(TextInputFormat.class); // record reader format and split procedure.
        conf.setOutputFormat(TextOutputFormat.class);
        System.out.println(Arrays.toString(args));
        FileInputFormat.addInputPath(conf , new Path(args[0])); // for the record redear in mapper
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.set("favourableFacilitiesCount", extraConfig[0]);
        conf.set("unFavourableFacilitiesCount", extraConfig[1]);
        conf.set("includeDistance", extraConfig[2]);

        JobClient client = new JobClient();

        RunningJob job = client.runJob(conf);

        while (!(job.isComplete() && job.isSuccessful())){
            try {
                Thread.sleep(500);
            }catch (InterruptedException exception){
                logger.fatal("The threads were interupted in between execution");
            }
        }

        JobConf conf2 = new JobConf(GaskyJob.class);
        conf2.setJobName("Distributed Skyline Reduction");
        // Set configurations for Job 2 as needed
        conf2.setMapperClass(UnionFacilityMapper.class);
        conf2.setReducerClass(FacilityCombinerReducer.class);
        conf2.setMapOutputKeyClass(GlobalOrderSkylineKey.class);
        conf2.setMapOutputValueClass(Text.class);
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);
        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.addInputPath(conf2, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
        conf2.set("favourableFacilitiesCount", extraConfig[0]);
        conf2.set("unFavourableFacilitiesCount", extraConfig[1]);
        conf2.set("includeDistance", extraConfig[2]);

        client.runJob(conf2);

        System.out.println("Elapsed Time :" + (System.currentTimeMillis() - time));
    }
}