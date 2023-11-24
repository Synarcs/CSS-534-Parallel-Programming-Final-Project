package com.css534.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

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
        conf2.setMapOutputKeyClass(Text.class);
        conf2.setMapOutputValueClass(Vector2f.class);
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);
        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.addInputPath(conf2, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf2, new Path(args[1]));

        RunningJob job2Status = client.runJob(conf2);

        System.out.println("Elapsed Time :" + (System.currentTimeMillis() - time));
    }
}