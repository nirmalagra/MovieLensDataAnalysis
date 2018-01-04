package com.kpi.mostviewed;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kpi.customtype.CustomWritable;
import com.kpi.util.ConstantUtil;

/* Created by Nirmal Jha
 * This class is used to parse the movie raw data  
 * */
public class MovieMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {
    private Text movieIdText= new Text(),movieTitleText = new Text();
    private final Text TYPE = new Text(ConstantUtil.TYPE_MOVIE);
    
    
    
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, CustomWritable>.Context context)
			throws IOException, InterruptedException {
		//MovieID::Title::Genres
		String[] movieData = value.toString().split(ConstantUtil.DATA_SEPERATOR);
	    if(movieData.length==3) {
	    	movieIdText.set(movieData[0]); 
	    	movieTitleText.set(movieData[1]); 
	    	context.write(movieIdText, new CustomWritable(TYPE, movieTitleText));
	    }
	}
}
