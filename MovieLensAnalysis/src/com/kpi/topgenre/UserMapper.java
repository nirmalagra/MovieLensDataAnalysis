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

public class UserMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {
	private final Text TYPE = new Text(ConstantUtil.TYPE_USER);
	private Text userIdText = new Text(), userDataText = new Text();
	Map<Integer, String> ageGroupMap = new AgeGroupMap();
	Map<Integer, String> professionMap = new ProfessionHashMap();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CustomWritable>.Context context)
			throws IOException, InterruptedException {
		// UserID::Gender::Age::Occupation::Zip-code
		String[] userData = value.toString().split(ConstantUtil.DATA_SEPERATOR);
		if (userData.length == 5) {
			userIdText.set(userData[0]);
			try {
				int ageGroupId = Integer.parseInt(userData[2]);
				int professionId = Integer.parseInt(userData[3]);
				// The age groups to be considered are: 18-35, 36-50 and 50+.

				if (ageGroupId == 18 || ageGroupId == 35 || ageGroupId == 50 || ageGroupId == 55) {
					StringBuilder userDataBuffer = new StringBuilder();
					userDataBuffer.append(ageGroupMap.get(ageGroupId)).append(ConstantUtil.DATA_SEPERATOR)
							.append(professionMap.get(professionId));
					userDataText.set(userDataBuffer.toString());
					context.write(userIdText, new CustomWritable(new Text(ConstantUtil.TYPE_USER), userDataText));
				}
			} catch (NumberFormatException ex) {
				ex.printStackTrace();
			}
		}
	}
}
