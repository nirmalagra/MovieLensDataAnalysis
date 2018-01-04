package com.kpi.mostviewed;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.kpi.customtype.CustomWritable;


/* Created by Nirmal Jha 
 * This class is actual MR job diver class, from here MR job will be submitted
 * */
public class MostViewedDriver {
	static String path1 = "mostviewed_intermediate";
	public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		try {
			if(args.length!=3) {
				System.err.println("<In1> <In2>  <out1>"); 
				System.exit(0);
			}
			FileUtils.deleteDirectory(new File(args[2]));
			Job job = new Job(configuration,"Intermediate Job");
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setJarByClass(MostViewedDriver.class);
			job.setReducerClass(MovieRatingReducer.class); 
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(CustomWritable.class);
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
			FileOutputFormat.setOutputPath(job, new Path(path1));
			int result =job.waitForCompletion(true) ? 1 : 0; 
			if(result ==1) {
				  /*
				   * Job 2 starts with output of first job
				   */
				  
				  Job job2 = new Job(configuration, "10 Most Viewed");
				  job2.setJarByClass(MostViewedDriver.class);
				  job2.setMapperClass(MostViewedMapper.class);
				  job2.setOutputKeyClass(NullWritable.class);
				  job2.setOutputValueClass(Text.class);
				  job2.setInputFormatClass(TextInputFormat.class);
				  job2.setOutputFormatClass(TextOutputFormat.class);
				  job2.setNumReduceTasks(0); 
				  TextInputFormat.addInputPath(job2, new Path(path1));
				  TextOutputFormat.setOutputPath(job2, new Path(args[2]));
				  int finalResultCode = job2.waitForCompletion(true) ? 0 : 1;
				  if(finalResultCode==0) {
					// delete intermediate o/p directories 
					  FileUtils.deleteDirectory(new File(path1));
				  }
				  System.exit(finalResultCode);
				  
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
