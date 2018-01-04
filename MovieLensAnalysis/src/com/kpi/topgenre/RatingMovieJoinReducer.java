package com.kpi.topgenre;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.kpi.customtype.CustomWritable;
import com.kpi.util.ConstantUtil;

public class RatingMovieJoinReducer extends Reducer<Text, CustomWritable, Text, Text> {
	private List<Text> userRatingList = new ArrayList<>();
	private Text movieData = new Text();
	private Text outValue = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<CustomWritable> values,
			Reducer<Text, CustomWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		userRatingList.clear();
		for (CustomWritable value : values) {
			if (value.getType().toString().equals(ConstantUtil.TYPE_MOVIE)) {
				movieData.set(value.getValue());
			} else if (value.getType().toString().equals(ConstantUtil.TYPE_RATING)) {
				userRatingList.add(new Text(value.getValue()));
			}	
		}
		if (movieData != null && !movieData.toString().isEmpty()) {
			String[] genres = movieData.toString().split(ConstantUtil.PIPE_SEPERATOR);
			for (Text ratingData : userRatingList) {
				for (String genre : genres) {
					outValue.set(genre);
					context.write(outValue, ratingData);
				}
				
			}
		}

	}
}
