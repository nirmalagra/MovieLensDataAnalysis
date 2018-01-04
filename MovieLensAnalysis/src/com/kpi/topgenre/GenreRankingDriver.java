package com.kpi.topgenre;

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
import com.kpi.util.ConstantUtil;

public class GenreRankingDriver {
	static String path1 = "intermediate1";
	static String path2 = "intermediate2";
	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		try {
			if(args.length!=4) {
				System.err.println("<In1> <In2> <In2> <out1> "); 
				System.exit(0);
			}
			// delete output and intermediate directories 
			FileUtils.deleteDirectory(new File(path1));
			FileUtils.deleteDirectory(new File(path2));
			FileUtils.deleteDirectory(new File(args[3]));
			
			Job job = new Job(configuration,"Intermediate Job 1");
			job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ConstantUtil.DATA_SEPERATOR);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setJarByClass(GenreRankingDriver.class);
			job.setReducerClass(UserRatingReducer.class); 
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(CustomWritable.class);
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
			FileOutputFormat.setOutputPath(job, new Path(path1));
			int result =job.waitForCompletion(true) ? 1 : 0; 
			if(result==1) {
				Job job2 = new Job(configuration,"Intermediate Job 2");
				job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", ConstantUtil.DATA_SEPERATOR);

				job2.setInputFormatClass(TextInputFormat.class);
				job2.setOutputFormatClass(TextOutputFormat.class);
				job2.setJarByClass(GenreRankingDriver.class);
				job2.setReducerClass(RatingMovieJoinReducer.class); 
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(CustomWritable.class);
				MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MovieMapper.class);
				MultipleInputs.addInputPath(job2, new Path(path1), TextInputFormat.class, UserRatingJoinMapper.class);
				FileOutputFormat.setOutputPath(job2, new Path(path2));
				int result2 =job2.waitForCompletion(true) ? 1 : 0; 
				//final job 
				if(result2==1) {
					Job job3 = new Job(configuration,"Most Ranked Genre");
					job3.getConfiguration().set("mapreduce.output.textoutputformat.separator", ConstantUtil.DATA_SEPERATOR);
					job3.setInputFormatClass(TextInputFormat.class);
					job3.setOutputFormatClass(TextOutputFormat.class);
					job3.setJarByClass(GenreRankingDriver.class);
					job3.setMapperClass(GenreRankingMapper.class);
					job3.setReducerClass(GenreRankingReducer.class); 
					job3.setOutputKeyClass(Text.class);
					job3.setOutputValueClass(Text.class);
					FileInputFormat.addInputPath(job3, new Path(path2));
					FileOutputFormat.setOutputPath(job3, new Path(args[3]));
					int finalResultCode = job3.waitForCompletion(true) ? 0 : 1;
					  if(finalResultCode==0) {
						// delete intermediate o/p directories 
						  FileUtils.deleteDirectory(new File(path1));
						  FileUtils.deleteDirectory(new File(path2));
					  }
					  System.exit(finalResultCode);
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
