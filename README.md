# Introduction to Big Data - Assignment 1

This repository contains the submission files of our group for the 1st Assignment of the Introduction to Big Data course.

Building results in two jars: Indexer and Query, for indexing text corpus and quering the search engine respectively. Both are based on Hadoop's MapReduce, and therefore require hadoop to run

## Running Indexer Jar:  
hadoop jar BM-Indexer-0.2.jar /EnWikiSmall bm-word-enumerator
bm-doc-counter bm-indexer-out

Arguments you need to provide after container name are:
1.  Path to directory containing text corpus
2.  Directory for Word Enumerator output
3.  Directory for Document Count output
4.  Directory for AverageDocLength output
5.  Directory for Indexer output

## Running Query Jar:  
hadoop jar BM-Query-0.1.jar word-enumerator-out bm-indexer-out
bm-av-doc-count bm-query-out "cats top" 5

Arguments you need to provide after container name are:
1.  Directory for WordEnumerator output
2.  Directory for Indexer output
3.  Directory for AverageDocLength output
4.  Directory for Query output
5.  Query text
6.  Number of most relevant documents to find