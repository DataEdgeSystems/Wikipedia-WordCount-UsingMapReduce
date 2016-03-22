Input Data Specification
Each line in the input file indicates a single entity in Wikipedia dataset. In each line, there are at most five elements divided by ‘tab’. The structure of each line is like this:

Articale_Unique_ID{\tab}Title_Of_Article{\tab}Date{\tab}Content{\tab}Extra Links

Note - 	The fifth element does not always exist.
	The ‘title’ and ‘date’ may contain ‘blanks’, so please do not use ‘blanks’ to split the string.
	The string may contain Unicode characters even it is generated from the English Wikipedia site.


Source Code
		
1. Wiki Word Count 
Find how many articles that contain a given specific keyword in the provided Wikipedia dataset.

2. Page Rank of Wiki User
Calculate the page rank value of a user in the Wikipedia vote network. Your program should output all the users and their page rank values into an HDFS file. Your program may need multiple iterations, and please use the third argument to specify the number of iterations.
