package com.kpi.toprated;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.kpi.util.ConstantUtil;

public class TopRatedMapper extends Mapper<Object, Text, NullWritable, Text> {
	private TreeMap<Double, Text> highestRated = new TreeMap<Double, Text>();

	@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
		String data = values.toString();
		String[] field = data.split(ConstantUtil.DATA_SEPERATOR);
		if (null != field && field.length == 2) {
			double views = Double.parseDouble(field[1]);
			highestRated.put(views, new Text(field[0] + ConstantUtil.DATA_SEPERATOR + field[1]));
			if (highestRated.size() > 20) {
				highestRated.remove(highestRated.firstKey());
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<Double, Text> entry : highestRated.descendingMap().entrySet()) {
			context.write(NullWritable.get(), entry.getValue());
		}
	}
}
