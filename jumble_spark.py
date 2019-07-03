#Assuming that our Jupyter Notebook is a driver program
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import udf, array, lit
from pyspark.sql.types import *

import json
import time
from collections import Counter

#Start the Spark context
sc = SparkContext("local[4]", "Jumble")
spark = SparkSession(sc)

#Load our dictionary into a Spark dataframe
with open('freq_dict.json') as json_file:
    frequency_dictionary = json.load(json_file)

df = sc.parallelize(frequency_dictionary.items()).toDF(['word', 'frequency'])

#Create a dictionary of words and their sorted characters to match
character_sort_udf = udf(lambda word: sorted(word), ArrayType(StringType()))
df_sorted_characters = df.withColumn("characters", character_sort_udf(df.word))

#Persist data as the primary lookup table
df_sorted_characters.persist(StorageLevel.MEMORY_AND_DISK)
df_sorted_characters.show()

#Simple lookup against our dictionary to return words that have the same charater sequences, given a sorted sequence
def find_words(query):
    return df_sorted_characters.where(df_sorted_characters.characters == query)

    #Given a string or an array of characters, return the character jumble from the given positions
def get_character_jumble(row, positions):
    jumble_characters = [row[position] for position in positions]
    return sorted(jumble_characters)
get_character_jumble_udf = udf(get_character_jumble, ArrayType(StringType()))

#Helper filter function based on count characters
def character_subset(row, query):
    counter_row = Counter(row)
    counter_query = Counter(query)
    counter_query.subtract(counter_row)
    return not any(char < 0 for char in dict(counter_query).values())
is_character_subset_udf = udf(character_subset,BooleanType())

#Iterate over the inputs
file_inputs = ['puzzle5.json'\
               ,'puzzle4.json'\
               ,'puzzle3.json'\
               ,'puzzle2.json'\
               ,'puzzle1.json'\
              ]
for file in file_inputs:
    print(file)
    with open(file) as json_file:
        puzzle = json.load(json_file)
    df_words = None
    #Figure out which characters we need from the input data
    character_positions = []
    offset = 0
    #Find possible answers to 1-word scrambles, then join to form combinations of the n 1-word scramble
    for key, value in puzzle[0].items():
        character_positions.extend(x+offset for x in value)
        if df_words is None:
            df_words = find_words(array(*(lit(x) for x in sorted(key))))
            df_words = df_words.select(df_sorted_characters.word.alias("word_merged"),\
                                       df_sorted_characters.frequency.alias("frequency_merged"),\
                                       df_sorted_characters.characters.alias("characters_merged"))
        else:
            df_words = df_words.crossJoin(find_words(array(*(lit(x) for x in sorted(key)))))
            df_words = df_words.select(functions.concat(df_words.word_merged, df_words.word).alias("word_merged"),\
                                      functions.concat(df_words.frequency_merged, lit(" "), df_words.frequency).alias("frequency_merged"),\
                                      functions.concat(df_words.characters_merged, df_words.characters).alias("characters_merged"))
        offset = offset + len(key)
    #Find character combinations for n 1-words
    df_words = df_words.select(get_character_jumble_udf(df_words.word_merged, array([lit(x) for x in character_positions])).alias("final_character_jumble")).distinct()
    df_words.show(20,False)

    #Find possible answers to successive words in the final scramble
    df_final_jumble = None
    for word_length in puzzle[1]:
        if df_final_jumble is None:
            df_final_jumble = df_words.crossJoin(df_sorted_characters.where(functions.length(df_sorted_characters.word) == word_length))\
            .filter(is_character_subset_udf(df_sorted_characters.characters, df_words.final_character_jumble))\
            .select(df_words.final_character_jumble,\
                    df_sorted_characters.word.alias("word_merged"),\
                    df_sorted_characters.frequency.alias("frequency_merged"),\
                    df_sorted_characters.characters.alias("characters_merged"))
        else:
            df_final_jumble = df_final_jumble.crossJoin(df_sorted_characters.where(functions.length(df_sorted_characters.word) == word_length))\
            .filter(is_character_subset_udf(df_sorted_characters.characters, df_final_jumble.final_character_jumble))\
            .select(df_final_jumble.final_character_jumble,\
                    functions.concat(df_final_jumble.word_merged, lit(" "), df_sorted_characters.word).alias("word_merged"),\
                    (df_final_jumble.frequency_merged + df_sorted_characters.frequency).alias("frequency_merged"),\
                    functions.sort_array(functions.concat(df_final_jumble.characters_merged, df_sorted_characters.characters)).alias("characters_merged")\
                   ).filter(is_character_subset_udf(df_final_jumble.characters_merged, df_final_jumble.final_character_jumble))

    #Implemented algo for threshold greater than 600
    df_final_jumble.where(df_final_jumble.final_character_jumble == df_final_jumble.characters_merged).where(df_final_jumble.frequency_merged > 600).sort("frequency_merged").limit(1).show()
