package nl.uva.cpp;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class HashtagCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	static enum Counters {
		INPUT_WORDS
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString().toLowerCase();

		// ignore T (timestamp) and U (user) lines
		if (!line.startsWith("w"))
			return;

		StringTokenizer itr = new StringTokenizer(line);
		while (itr.hasMoreTokens()) {

			// Obtain the next word.
			String token = itr.nextToken();

			// ignore non-hashtag tokens
			if (!token.startsWith("#"))
				continue;

			// Write (word, 1) as (key, value) in output
			word.set(token);
			context.write(word, one);

			// Increment a counter.
			context.getCounter(Counters.INPUT_WORDS).increment(1);
		}
	}
}
