package com.awl;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values, Context context)
	      throws IOException, InterruptedException {
		  
		  long wordlengthsum = 0;
		  long wordcount = 0;
		  
		  for(IntWritable value : values) {
			  wordlengthsum += value.get();
			  wordcount++;
		  }
		  
		  if (wordcount != 0) {
			  double result = (double) wordlengthsum / (double)wordcount;
			  
			  context.write(key, new DoubleWritable(result));
		  }
	  }
	}
