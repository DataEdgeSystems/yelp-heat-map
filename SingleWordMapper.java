package solution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class SingleWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //declare heap variables outside the map method
	Text busIdToSave = new Text();
	String wordToMap = null;
	IntWritable count = new IntWritable(1);
	  
	  @Override
	    protected void setup(Context context){
	        Configuration conf = context.getConfiguration();
	        wordToMap = conf.get("yelp.wordtomap");
	        if (wordToMap == null) {
	            throw new RuntimeException("Error reading the word to map");

	        }
	  }


	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	
    String line = value.toString();
    Object obj=JSONValue.parse(line);
    JSONObject json = (JSONObject) obj;
    String reviewText = (String) json.get("text");
    String businessId = (String) json.get("business_id");
    busIdToSave.set(businessId);
    /*
     * The line.split("\\W+") call uses regular expressions to split the
     * line up by non-word characters.
     * 
     */
    for (String word : reviewText.split("\\W+")) {
      if (word.length() > 0) {
        
    	 
    	if(wordToMap.toLowerCase().equals(word.toLowerCase()))
    	{
            context.write(busIdToSave, count);
    	}    	 	  
    	
      }
    }
  }
}