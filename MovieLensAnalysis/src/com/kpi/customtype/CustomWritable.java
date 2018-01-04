package com.kpi.customtype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/* Created by Nirmal Jha
 * This is custom data type class  which identify if data is for rating/movie/user at reducer
 * */
public class CustomWritable implements Writable{
	
	private Text value;
	private Text type;
	public CustomWritable() {
		value = new Text();
		type = new Text();
	}
	public CustomWritable(Text type, Text title) {
		this.type = type;
		this.value = title;
	}
	
	
	public Text getValue() {
		return value;
	}

	public void setValue(Text value) {
		this.value = value;
	}

	public Text getType() {
		return type;
	}

	public void setType(Text type) {
		this.type = type;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		type.readFields(in);
		value.readFields(in); 
	}

	@Override
	public void write(DataOutput out) throws IOException {
		type.write(out);
		value.write(out);
	}

}
