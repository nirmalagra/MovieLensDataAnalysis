package com.kpi.topgenre;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kpi.customtype.CustomWritable;
import com.kpi.util.AgeGroupMap;
import com.kpi.util.ConstantUtil;
import com.kpi.util.ProfessionHashMap;

public class RatingMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {

	private final Text TYPE = new Text(ConstantUtil.TYPE_RATING);
	private Text userIdText = new Text(), ratingDataText = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CustomWritable>.Context context)
			throws IOException, InterruptedException {
		// UserID::MovieID::Rating::Timestamp
		String[] userData = value.toString().split(ConstantUtil.DATA_SEPERATOR);
		if (userData.length == 4) {
			userIdText.set(userData[0]);
			StringBuilder ratingDataBuffer = new StringBuilder();
			ratingDataBuffer.append(userData[1]).append(ConstantUtil.DATA_SEPERATOR).append(userData[2]);
			ratingDataText.set(ratingDataBuffer.toString());
			context.write(userIdText, new CustomWritable(TYPE, ratingDataText));
		}
	}
}
