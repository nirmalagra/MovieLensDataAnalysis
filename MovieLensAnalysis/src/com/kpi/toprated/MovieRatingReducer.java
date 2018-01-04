package com.kpi.toprated;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.kpi.customtype.CustomWritable;
import com.kpi.util.ConstantUtil;

public class MovieRatingReducer extends Reducer<Text, CustomWritable, Text, Text> {
	private String pattern = "#.##";
	DecimalFormat decimalFormat;
	private Text movieTitle = new Text();

	@Override
	protected void setup(Reducer<Text, CustomWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		decimalFormat = new DecimalFormat(pattern);
		super.setup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<CustomWritable> values,
			Reducer<Text, CustomWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		List<Integer> ratingList = new ArrayList<>();
		for (CustomWritable value : values) {
			if (value.getType().toString().equals(ConstantUtil.TYPE_MOVIE)) {
				movieTitle.set(value.getValue());
			} else if (value.getType().toString().equals(ConstantUtil.TYPE_RATING)) {
				ratingList.add(Integer.parseInt(value.getValue().toString()));
			}
		}
		StringBuffer outputBuffer = new StringBuffer();
		if (movieTitle != null && !movieTitle.toString().isEmpty() && ratingList.size() >= 40) {
			double sum = 0;
			for (Integer rating : ratingList) {
				sum += rating;
			}
			double average = sum / ratingList.size();
			outputBuffer.append(decimalFormat.format(average));
			context.write(movieTitle, new Text(outputBuffer.toString()));
		}
	}
}
