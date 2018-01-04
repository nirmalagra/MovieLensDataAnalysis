package com.kpi.topgenre;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kpi.customtype.CustomWritable;
import com.kpi.util.ConstantUtil;

public class UserRatingJoinMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {

	private final Text TYPE = new Text(ConstantUtil.TYPE_RATING);
	private Text movieIdText = new Text(), ratingDataText = new Text();
	private Text outvalue = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CustomWritable>.Context context)
			throws IOException, InterruptedException {
		// 35-44::academic/educator::1091::3
		String[] userData = value.toString().split(ConstantUtil.DATA_SEPERATOR);
		if (userData.length == 4) {
			movieIdText.set(userData[2]);
			//StringBuffer ratingDataBuffer = new StringBuffer();
			outvalue.set(userData[0] + "::" + userData[1]+"::"+userData[3]);
			context.write(movieIdText, new CustomWritable(new Text(ConstantUtil.TYPE_RATING), outvalue));
		}
	}
}