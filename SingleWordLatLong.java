package solution;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SingleWordLatLong extends Configured implements Tool {
    final static String BUSINESS_MAPPING_FILE = "yelp.business.file";
    
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
 
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        String bus_file_name = conf.get(BUSINESS_MAPPING_FILE);
        DistributedCache.addCacheFile(new Path(bus_file_name).toUri(), conf); 
        
        Job job = new Job(conf, "Yelp Lat Long Job");
        job.setJarByClass(SingleWordLatLong.class);
    	
        job.setJobName("Single Word Lat Long Job");
        
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        
        job.setMapperClass(SingleWordMapper.class);
        job.setReducerClass(SingleWordReducer.class);        

        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Configuration(), new SingleWordLatLong(), args);
        System.exit(exitCode);        
    }
   
}