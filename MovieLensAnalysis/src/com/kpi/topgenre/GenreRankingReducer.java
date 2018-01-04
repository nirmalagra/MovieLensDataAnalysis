package com.kpi.topgenre;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.kpi.util.ConstantUtil;

public class GenreRankingReducer extends Reducer<Text, Text, Text, Text> {
	private Map<String, GenreRankModel> genreRatingMap = new HashMap<>();
	private TreeMap<Double, String> finalMap = new TreeMap<>();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		genreRatingMap.clear();
		finalMap.clear();
		for (Text value : values) {
			// Animation::5
			String[] valueArr = value.toString().split(ConstantUtil.DATA_SEPERATOR);
			String genreName = valueArr[0];
			int rating = Integer.parseInt(valueArr[1]);
			GenreRankModel ranking = genreRatingMap.get(genreName);
			if (ranking != null) {
			ranking.setSum(ranking.getSum() + rating);
			ranking.setCount(ranking.getCount() + 1);
			} else {
			GenreRankModel rankingNew = new GenreRankModel();
			rankingNew.setSum(rating);
			rankingNew.setCount(1);
			genreRatingMap.put(genreName, rankingNew);
			}
		}

		for (Map.Entry<String, GenreRankModel> entry : genreRatingMap.entrySet()) {
			GenreRankModel gr = entry.getValue();
			double average = gr.getSum() / gr.getCount();
			finalMap.put(average, entry.getKey());
		}
		StringBuilder sb = new StringBuilder();
		int size = finalMap.size();
		int currentPos=0;
		for (Map.Entry<Double, String> entry : finalMap.descendingMap().entrySet()) {
			currentPos++;
			if(size!=currentPos) {
			sb.append(entry.getValue()).append("|");
			} else {
				sb.append(entry.getValue());
			}
		}
		context.write(key, new Text(sb.toString()));
	}

}
