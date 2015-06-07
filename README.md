# yelp-heat-map
MapReduce algorithm for Yelp's Academic Dataset to create a geographic heatmap of a word

The goal of this analysis is to show which words occur frequently at a particular geographic region. The output is a HeatMap that shows the frequency at which any arbitrary word occurs at a particular latitude and longitude. In order to display that HeatMap, for each word, we need a list of all the coordinates that the word appears at and the number of times it appears there. The table below shows the MapReduce data flow to get the list of geographic coordinates at which a single word appears:


| Mapper In | Mapper Output/Reducer Input	| Reducer Output| 
| ------------- | ----------- | ----------- | 
| Review JSON structure |Each time the desired word is found, produce <br /> Key(String): Business_ID  <br /> Value (IntWritable):1	| Key(String):[latitude, longitude] <br />Value(IntWritable) :frequency |


Currently has been tested using the JSON data for reviews and business provided by Yelp here: https://www.yelp.com/academic_dataset

To run the program, you'll also need to get the json-simple library and pass it as a param to libjars.

<pre>hadoop jar &#60built_solution.jar&#62  solution.WordCount -libjars=&#60 path to simple json&#62 /json-simple-1.1.1.jar  -D wc.business.file=&#60 path to business json file&#62  &#60 path to review files directory&#62  &#60 outputdir&#62</pre>

The output is a format easily consumable by google heatmaps api. See https://developers.google.com/maps/documentation/javascript/heatmaplayer
