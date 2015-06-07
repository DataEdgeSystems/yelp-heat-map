package solution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Test;

public class TestSingleWordLatLong {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and a mapper and
	 * a reducer working together.
	 */
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, NullWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, NullWritable> mapReduceDriver;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		SingleWordMapper mapper = new SingleWordMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		SingleWordReducer reducer = new SingleWordReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, NullWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, NullWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);

		// Set a temporary configuration so we don't throw an exception
		Configuration config = new Configuration();
		config.set("yelp.wordtomap", "dog");		

		mapDriver.setConfiguration(config);
		reduceDriver.setConfiguration(config);
		mapReduceDriver.setConfiguration(config);
		
		// When MR Unit runs the test, the files don't actually get added to
		// cache at the normal point they would in execution, so
		// We need to manually add the Cache files for testing
		mapDriver
				.addCacheFile("/home/training/workspace/yelpdata/business.json");

	}

	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper() throws IOException {

		/*
		 * For this test, the mapper's input will be a review with 3 words
		 */
		mapDriver.withInput(new LongWritable(1), new Text(
				"{\"text\":\"cat and dog dog\",\"business_id\":\"JwUE5GmEO-sH1FuwJgKBlQ\"}"));
		mapDriver.withOutput(new Text("JwUE5GmEO-sH1FuwJgKBlQ"), new IntWritable(1));
		mapDriver.withOutput(new Text("JwUE5GmEO-sH1FuwJgKBlQ"), new IntWritable(1));

		mapDriver.runTest();
	}

	/*
	 * Test the reducer.
	 */
	@Test
	public void testReducer() throws IOException {

		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));		
		reduceDriver.withInput(new Text("wUE5GmEO-sH1FuwJgKBlQ"), values);
		reduceDriver.withOutput(new Text("{location: new google.maps.LatLng(43.238892999999997, -89.335843999999994), weight: 2}"), NullWritable.get());
		reduceDriver.runTest();
	}

	/*
	 * Test the mapper and reducer working together.
	 */
	@Test
	public void testMapReduce() throws IOException {

		mapReduceDriver.withInput(new LongWritable(1), new Text(
				"{\"text\":\"cat and dog dog\",\"business_id\":\"JwUE5GmEO-sH1FuwJgKBlQ\"}"));
	
		
		mapReduceDriver.withOutput(new Text("{location: new google.maps.LatLng(43.238892999999997, -89.335843999999994), weight: 2}"), NullWritable.get());
		mapReduceDriver.runTest();

	}
}
