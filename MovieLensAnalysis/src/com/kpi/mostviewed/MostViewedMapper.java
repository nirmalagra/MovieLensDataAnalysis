package com.kpi.mostviewed;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kpi.util.ConstantUtil;

public class MostViewedMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
	
	private TreeMap<Integer, Text> movieDataMap = new TreeMap<Integer, Text>();
	private final int RECORD_COUNT = 10;
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		//Movie Title: Count
		String[] movieData = value.toString().split(ConstantUtil.DATA_SEPERATOR);
		if(movieData.length==2) {
			movieDataMap.put(Integer.parseInt(movieData[1]), new Text(value));
		}
		if(movieDataMap.size()>RECORD_COUNT) { 
			movieDataMap.remove(movieDataMap.firstKey());
		}
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for(Entry<Integer,Text> entry:movieDataMap.descendingMap().entrySet()) {
			System.out.println("Output: "+entry.getValue().toString());
			context.write(NullWritable.get(), entry.getValue());  
		}
	}
}
