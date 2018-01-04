package com.kpi.mostviewed;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kpi.customtype.CustomWritable;
import com.kpi.util.ConstantUtil;

/* Created by Nirmal Jha
 * RatingMapper class will be used to parse the rating raw data 
 * */
public class RatingMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {
    private Text movieIdText = new Text(),rating = new Text();
    private final Text TYPE = new Text(ConstantUtil.TYPE_RATING);
    
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, CustomWritable>.Context context)
			throws IOException, InterruptedException {
		//UserID::MovieID::Rating::Timestamp
		String[] ratingData = value.toString().split(ConstantUtil.DATA_SEPERATOR);
	    if(ratingData.length==4) {
	    	movieIdText.set(ratingData[1]); 
	    	rating.set(ratingData[2]); 
	    	context.write(movieIdText, new CustomWritable(TYPE, rating));
	    } 
	}
}
