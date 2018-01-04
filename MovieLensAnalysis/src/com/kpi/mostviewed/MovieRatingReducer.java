package com.kpi.mostviewed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.kpi.customtype.CustomWritable;
import com.kpi.util.ConstantUtil;
/* Created by Nirmal Jha
 * This reducer class will be used to reduce the output from MovieMapper and RatingMapper class.
 * */
public class MovieRatingReducer extends Reducer<Text, CustomWritable, NullWritable, Text>{

	@Override
	protected void reduce(Text key, Iterable<CustomWritable> values,
			Reducer<Text, CustomWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String movieTitle = "";
       List<String> ratingList = new ArrayList<>();
	    for(CustomWritable value: values) {
	    	if(value.getType().toString().equals(ConstantUtil.TYPE_MOVIE)) {
	    		movieTitle = value.getValue().toString();
	    	}else if(value.getType().toString().equals(ConstantUtil.TYPE_RATING)) {
	    		ratingList.add(value.getValue().toString());
	    	}
	    }
	    StringBuffer outputBuffer = new StringBuffer(movieTitle);
	    outputBuffer.append(ConstantUtil.DATA_SEPERATOR).append(ratingList.size());
	    context.write(NullWritable.get(), new Text(outputBuffer.toString()));
	}
}
