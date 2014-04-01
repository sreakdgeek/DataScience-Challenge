DataScience-Challenge
=====================

Facebook data analysis - part I

1) Naive Histogram implementation:
 https://github.com/sreakdgeek/DataScience-Challenge/blob/master/naive_histograms_like.py

2) Map-Reduce Version:
https://github.com/sreakdgeek/DataScience-Challenge/blob/master/MRHistogram.py

Initial version builds the histogram as a dictionary. However, this would not be very scalable. In my 2nd version, I have created a Map Reduce program using mrjob framework. Also tested this version on AWS EMR. Also to handle differently encoded characters in the data, encoding scheme was detected using chardet module and accordingly characters are decoded to unicode.
