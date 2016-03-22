Input Data Specification
Each line in the input file indicates a single entity in Wikipedia dataset. In each line, there are at most five elements divided by ‘tab’. The structure of each line is like this:

Articale_Unique_ID{\tab}Title_Of_Article{\tab}Date{\tab}Content{\tab}Extra Links

Note - 	The fifth element does not always exist.
	The ‘title’ and ‘date’ may contain ‘blanks’, so please do not use ‘blanks’ to split the string.
	The string may contain Unicode characters even it is generated from the English Wikipedia site.

The input file is a text file that consists of many articles and their content. Each article and its related information is stored in only ONE line of the input text file. It means that each line contains all of the information for one article. For example, if an input text file has 100 lines, it means that the file has 100 articles.

Source Code
		
1. CheckKeywordWiki.java 
Find how many articles that contain a given specific keyword in the provided Wikipedia dataset.

2. Top5FrequentWordsInWikiArticle.java
Twist the WordCount program and develop a new program that finds the five most frequent words in all articles whose title contains the given keyword.
