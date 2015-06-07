package solution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class SingleWordReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

	
	Text outputText = new Text();
	NullWritable nullOutput;

	Map<String, String> busMap = new HashMap<String, String>();

	  private void readBusinessMapping(String bus_file_name){
		  ///Set up the business-location mapping
	        if(bus_file_name == null){
	            throw new RuntimeException("Could not find business file in config");
	        }
	        File bus_file = new File(bus_file_name);
	        
	        BufferedReader reader;
	        try {
	        	reader = new BufferedReader(new FileReader(bus_file));
	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	            throw new RuntimeException("Could not open business file ",e);
	        }
	        String line;
	        try {
	            while((line =reader.readLine()) != null){
	            	 Object obj=JSONValue.parse(line);
	            	 JSONObject json = (JSONObject) obj;
	            	 String latitude = String.valueOf(json.get("latitude"));
	            	 String longitude = String.valueOf(json.get("longitude"));
	            	 String businessId = (String) json.get("business_id");
	            	 busMap.put(businessId, "(".concat(latitude).concat(",").concat(longitude).concat(")"));
	            	 }
	        } catch (IOException e) {
	            e.printStackTrace();
	            throw new RuntimeException("error while reading business",e);
	        }
	  }
	  
	  @Override
	    protected void setup(Context context){

	        Configuration conf = context.getConfiguration();
	        String bus_file_name = null;	      
	        
	        try {
				Path files[] = DistributedCache.getLocalCacheFiles(conf);
				
				for (Path filename : files)
				{
					System.out.println("MME Cache file:" + filename.toString());
					if(filename.getName().contains("business"))
					{
						bus_file_name = filename.toString();
					}
				}
				
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	        
	        readBusinessMapping(bus_file_name);                
	      
	  }
	  
	  /*
	   * Reduce will be called once for each business ID that contains the word. THe reduce method should sum up the frequency that the word is used at that location and then
	   * print out that weight. Reduce is also responsible for translating the business ID into a latitude/longitude value
	   */
  @Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
	  int frequency = 0;
	  String latLong = busMap.get(key.toString());
	  

	  //Desired output is {location: new google.maps.LatLng(37.782, -122.443), weight: 2},
		
	  
	  
	  for (IntWritable value : values) {
		  
		  frequency+=value.get();
		}
	  
	  String outputStr = "{location: new google.maps.LatLng" + latLong + ", weight: " + frequency + "},";
	  outputText.set(outputStr);
	  
	  context.write(outputText, nullOutput);
	}
}