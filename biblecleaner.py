
#1 Importing regular expression library to use
import re

#2 Importing the contents of the bible into an RDD
bible = sc.textFile("\Users\Rohit\Desktop\ascii_bible.txt")

#3 Replacing tabs and newline expressions with a space, and then separating the words by space
biblewords = bible.map(lambda newline: newline.replace("\n"," ")).map(lambda newline: newline.replace("\t"," ")).flatMap(lambda words: words.split(" "))

#4 Removing verses by searching for the 'MNM:MNM' pattern
verseless_bible = biblewords.map(lambda x: re.sub('[0-9][0-9][0-9]:[0-9][0-9][0-9]',"",x))

#5 Removing punctuations from the start of the word using ^ and end of the word using $
cleanwords = verseless_bible.map(lambda x: re.sub(r'[,|.|!|?|:|;|(|)|]$','',x)).map(lambda x: re.sub(r'^[,|.|!|?|:|;|(|)|]','',x))

#6 Filtering words only longer than 2 words
bigwords = cleanwords.filter(lambda words: len(words)>2)

#7 Converting all words to lowercase words
lowercase = bigwords.map(lambda words: words.lower())

#8 Removing all the 's from words
singularwords = lowercase.map(lambda x: re.sub(r'\'s$','', x))

#9 Mapping these words and emitting (word,1)
mapper = singularwords.map(lambda x: (x,1))

#10 Reducing values to get frequencies
frequency = mapper.reduceByKey(lambda x,y: x+y)

#Outputs 1 - Saving output as 'ans.txt'
frequency.saveAsTextFile("/Users/Rohit/Desktop/ans.txt")

#Outputs 2 - top 10 words in descending order
frequency.sortBy(lambda x: x[1], False).take(10)

#Outputs 3 - top 10 words in ascending order
frequency.sortBy(lambda x: x[1], True).take(10)

#Outputs 4 - Count of final RDD
frequency.count()
12768




