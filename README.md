# coding_challenge_insight_DE

This program focuses on storage efficiency for processing within evry tweet and time efficiency while processing successive tweets. It minimizes the use of string storage and comparisons. It operates tweet by tweet to calulate number of distinct words, their median and their frequency. A unique hash key is calculated for every word within each tweet followed by calculation of two word weights by summing ASCII values of all characters in the unique hash key and summing over the first 4 characters of unique hash key for making every word disticnctly countable and seperable with min comparisons and storage. The program is scalable for zny size of file with any number of tweets and focusses on use of pandas dataframes in python which is a scalable datastructure preferred in big data type of operations. The example input tweets and corresponsing output tweets have been obtained using this script in github repositories. Its a well thought innovative approach which I believe provides a good technical tradeoff and balance of time and storage efficiency. 

NOTE: This script and installation steps are developed and recommended using Ubuntu Linux OS platform
It takes input tweets from 'tweets.txt' file in tweet_input directory and prints the, 1) word and their respective frequency in 'ft1.txt' file and, 2) distinct words and running median per tweet in 'ft2.txt' file in output_tweet directory

Its completely scalable and any size of input file can be tested as by updating the inputs in 'tweets.txt' file in tweet_input directory.

