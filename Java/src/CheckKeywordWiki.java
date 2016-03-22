package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * Program to check for a keyword in a wiki article's title or content.
 * 
 * It requires 3 arguments as input.
 * Input_File - The wiki dataset/text file.
 * Keyword - the word to search for in the Input_File
 * Output_Directory - The location of the Reducer Output
 *
 * @author Sarang
 */
public class CheckKeywordWiki 
{

	/**
	 * Map class for this program. It extends the class MapReduceBase and implements the interface Mapper.
	 * It contains the logic for the Map phase of this MapReduce Job.
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
 	 {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		// Variable to store the keyword passed as argument to the program
		private String keyword = null;
	
		/**
		 * A method of class MapReduceBase. It allows us to access variables set in the configuration object of this MapReduce Job.
		 */
		public void configure(JobConf job)
		{
			super.configure(job);
			keyword = (job.get("keyword"));  // Accessing the variable "keyword" set in the Configuration Object.
		}

		/**
		 * A method of the interface Mapper. Maps a single input <key,value> pair into an intermediate <key,value> pair
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
	 	 {
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
						//word.set("The keyword " + keyword + " is present in article titled: " + ArticleTitle);
						word.set(keyword);
						output.collect(word,one);
						hit = true;
						break;
				 	}
			 	}

				// If the word is already found in Title then ignore Content. Else check for the word in content.
				if(!hit)
			 	{
					// Capturing all the words in Content
					StringTokenizer ContentTokens = new StringTokenizer(ArticleContent.toLowerCase());

					// Checking if the keyword is present in the Content
					while(ContentTokens.hasMoreTokens())
			 		{
						String ContentToken = ContentTokens.nextToken();
						if(ContentToken.equals(keyword.toLowerCase()))
				 		{
							//word.set("The keyword " + keyword + " is present in article titled: " + ArticleTitle + ". The content word is " + ContentToken );
							word.set(keyword);
							output.collect(word,one);
							break;
					 	}
			 		}
			 	}
			} //End of IF
			else if(keywordTokenCount > 1)
			{
				boolean hit = false;
				ArticleTitle = ArticleTitle.toLowerCase();
				
				if(ArticleTitle.contains(keyword.toLowerCase()))
				{
					//word.set(keyword + " found in Article: " + ArticleTitle);
					word.set(keyword);
					output.collect(word,one);
					hit = true;
				}
				
				if(!hit)
				{
					ArticleContent = ArticleContent.toLowerCase();
					if(ArticleContent.contains(keyword.toLowerCase()))
					{	
						//word.set(keyword + " found in content with Title: " + ArticleTitle);
						word.set(keyword);
						output.collect(word,one);
					}
				}
			}

	 	 } // End of map method
 	 } // End of  Map class


	/**
	 * Reduce class for this program. It extends the class MapReduceBase and implements the interface Reducer.
	 * It contains the logic for Reduce phase of this MapReduce Job.
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> 
	 {
		/**
		 * A method of the interface Reducer. It reduces values for a given key. Output Values must be the same type as Input Values.
		 */
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
		 {
			int sum = 0;

			// Looping through all the values for a particular KEY received from MAP Phase			
			while (values.hasNext()) 
			 {
				sum += values.next().get(); // Process value i.e. it sums up all the values for a particular key.
			 }

			// Collects the output of each reducer and writes it to the HDFS FileSystem
			output.collect(key, new IntWritable(sum));
		 }
	}

	public static void main(String[] args) throws Exception 
	 {
		//Creating an instance of the class JobClient. It allows us to submit jobs and track their progress.
		JobClient client = new JobClient();

		//Creating an instance of the class JobConf. It stores the configuration parameters for our MapReduce Job. 
		JobConf conf = new JobConf(Prog1.class);
		conf.setJobName("Programming Project 1");

		// Setting the JobConf object to store a user parameter. This can then be accessed in the Mapper/Reducer.
		conf.set("keyword",args[1]);
		
		// Setting the number of reducers
		conf.setNumReduceTasks(1);

		// Specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		// Specify a Mapper
		conf.setMapperClass(Map.class);

		// Specify a Reducer
		conf.setReducerClass(Reduce.class);

		// Specify Input Types
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		//Specify Input and Output directories
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		client.setConf(conf);
		
		// Submitting the Job
		try
		{
			JobClient.runJob(conf);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	 } // End of main

}
