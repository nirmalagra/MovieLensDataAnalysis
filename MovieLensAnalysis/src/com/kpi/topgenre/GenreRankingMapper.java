package com.kpi.topgenre;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kpi.util.ConstantUtil;

public class GenreRankingMapper extends Mapper<LongWritable, Text, Text, Text>{
    private Text keyText = new Text();
    private Text valueText = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
	//Animation::18-35::programmer::5
		String[] dataArr = value.toString().split(ConstantUtil.DATA_SEPERATOR);
		if(dataArr.length==4) {
			keyText.set(dataArr[2].concat(ConstantUtil.DATA_SEPERATOR).concat(dataArr[1]));  
			valueText.set(dataArr[0].concat(ConstantUtil.DATA_SEPERATOR).concat(dataArr[3]));  
			context.write(keyText, valueText);
		}
	}
}
