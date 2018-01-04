package com.kpi.topgenre;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.kpi.customtype.CustomWritable;
import com.kpi.util.ConstantUtil;

public class UserRatingReducer extends Reducer<Text, CustomWritable, Text, Text> {
	private List<Text> ratingList = new ArrayList<>();
	private Text userData = new Text();

	@Override
	protected void reduce(Text key, Iterable<CustomWritable> values,
			Reducer<Text, CustomWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		ratingList.clear();

		for (CustomWritable value : values) {
			if (value.getType().toString().equals(ConstantUtil.TYPE_USER)) {
				userData.set(value.getValue());
			} else if (value.getType().toString().equals(ConstantUtil.TYPE_RATING)) {
				ratingList.add(new Text(value.getValue()));
			}
		}
		if (userData != null && !userData.toString().isEmpty()) {
			for (Text ratingData : ratingList) {
				context.write(userData, ratingData);
			}
		}

	}

}
