package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/**
 * Program to return the top 5 most frequent words in each Wiki Article whose title contains the given keyword.
 * 
 * It requires 3 arguments as input.
 * Input_File - The wiki dataset/text file.
 * Keyword - the word to search for in the Input_File
 * Output_Directory - The location of the Reducer Output
 *
 * @author Sarang
 */
public class Top5FrequentWordsInWikiArticle
{

	/**
	 * Map class for this program. It extends the class Mapper.
	 * It contains the logic for the Map phase of this MapReduce Job.
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
 	 {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		// Variable to store the keyword passed as argument to the program
		private String keyword = null;

		/**
		 * A method of the interface Mapper. Maps a single input <key,value> pair into an intermediate <key,value> pair
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	 	 {
			Configuration conf = context.getConfiguration();
			keyword = conf.get("keyword");
			
			StringTokenizer keywordTokens = new StringTokenizer(keyword);
			int keywordTokenCount = keywordTokens.countTokens();

			String line = value.toString();

			// Splitting each line based on TAB Character			
			String[] parts = line.split("\t");

			// Article format:  ArticleID  TAB  ArticleTitle  TAB  ArticleDate  TAB  ArticleContent  TAB  ArticleExtras
			// Extracting ArticleTitle and ArticleContent 
			String ArticleTitle = parts[1];
			String ArticleContent = parts[3];
 
			if( keywordTokenCount == 1 )
			{
				// Capturing all the words in the title
				StringTokenizer TitleTokens = new StringTokenizer(ArticleTitle.toLowerCase());
			
				boolean hit = false;

				// Checking if the keyword is present in the Title
				while(TitleTokens.hasMoreTokens())
			 	{
					String TitleToken = TitleTokens.nextToken();
					if(TitleToken.equals(keyword.toLowerCase()))
				 	{						
						hit = true;
						break;
				 	}
			 	}

				// If the word is found in Title then run word count on Content.
				if(hit)
			 	{
					// Capturing all the words in Content
					StringTokenizer ContentTokens = new StringTokenizer(ArticleContent.toLowerCase());

					// Word Count for every token in Content
					while(ContentTokens.hasMoreTokens())
			 		{
						String ContentToken = ContentTokens.nextToken();
						word.set("ContentToken: " + ContentToken);
						context.write(word,one);
			 		}
			 	}
			} //End of IF
			else if(keywordTokenCount > 1)
			{
				boolean hit = false;
				ArticleTitle = ArticleTitle.toLowerCase();
				
				if(ArticleTitle.contains(keyword.toLowerCase()))
				{					
					hit = true;
				}
				
				if(hit)
				{
					//capturing all tokens in Content
					StringTokenizer ContentTokens = new StringTokenizer(ArticleContent.toLowerCase());

					// Word Count for every token in Content
					while(ContentTokens.hasMoreTokens())
					{	
						String ContentToken = ContentTokens.nextToken();
						word.set("ContentToken: " + ContentToken);
						context.write(word,one);
					}
				}
			}

	 	 } // End of map method
 	 } // End of  Map class


	/**
	 *
	 */
	public static class WordComparator implements Comparator
	{
		HashMap map = new HashMap();

		public WordComparator(HashMap map)
		{
			this.map = map;
		}
		
		public int compare(Object one, Object two)
		{
			return (((Integer)map.get(two)).compareTo((Integer)map.get(one)));
		}
	}


	/**
	 * Reduce class for this program. It extends the class MapReduceBase and implements the interface Reducer.
	 * It contains the logic for Reduce phase of this MapReduce Job.
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
	 {
		//private IntWritable result = new IntWritable();
		private HashMap<Text, Integer> map = new HashMap<Text, Integer>();

		/**
		 * A method of the interface Reducer. It reduces values for a given key. Output Values must be the same type as Input Values.
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		 {
			int sum = 0;

			for(IntWritable val: values)
			{
				sum+=val.get();
			}			
			
			//result.set(sum);
			//context.write(key,result);
			map.put(key,sum);
		 }

		public void cleanup(Context context) throws IOException, InterruptedException
		{
			int count = 0;
			WordComparator comp = new WordComparator(map);
			TreeMap<Text,Integer> sortedMap = new TreeMap(comp);
			sortedMap.putAll(map);

			for(Text entry : sortedMap.keySet())
			{
				while(count<5)
				{
					context.write(entry,new IntWritable(sortedMap.get(entry)));
					count++;
				}
			}
		}

	}

	public static void main(String[] args) throws Exception 
	 {

		Configuration conf = new Configuration();
		conf.set("keyword",args[1]);

		//Creating an instance of the class Job. It stores the configuration parameters for our MapReduce Job and submits the Job.  
		Job job = new Job(conf);
		job.setJarByClass(Prog3.class);
		
		// Setting the Configuration for the Job.
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);		

	 } // End of main

}
