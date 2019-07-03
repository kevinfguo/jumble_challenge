# Jumble Challenge
This is a take-home challenge for a Big Data role. The instructions provided are included as `SparkExercise.txt`

There are 5 jumble puzzles, puzzle[1-5].jpg that were transformed into JSON format for ingestion, puzzle[1-5].json with the following format:

List[
Dictionary {key: "1-word jumble", value: List [final jumble character positions]},
List [final jumble word lengths]
]

Included are solution files:
*Jumble.ipynb*
jumble_spark.py (derived from Jumble.ipynb)

This challenge was completed running the included Jyupter notebook. Relevant library versions:

Anaconda 4.6.14

PySpark 2.4.3

Jupyter 1.0.0

I would recommend running the Jumble notebook, to easily replicate the results and output, but you are free to treat the notebook as a driver to submit via command line, hence the provided jumble_spark.py file.

## The Solution
1) Leveraging the included freq_dict.json to create a dictionary of word-frequency key-value pairs; we load this into a Spark Dataframe. This dataframe is augmented with the sorted characters of the word to provide quick lookup for words that can match a sorted character jumble. This dataframe, df_sorted_characters, is cached as the primary lookup table.

2) For each Jumble puzzle...

3) Take the 1-word jumbles, find every word that is a possible match by sorting the character jumble and matching against the lookup table, df_sorted_characters. Do the same lookup for each successive 1-word jumble and join these results to form all possible combinations of the 1-word jumbles.

4) Get the possible characters for the final jumble from all combinations, and take their distinct results as some may be repeating

5) Now, for all of these character combinations for the final jumble, join against possible n-letter words which have characters that are a subset of the final jumble characters.

6) Do this for successive m-letter words, merging the character list and ensuring they remain a subset of the final jumble characters

7) This returns all combinations of words that are candidates for the final jumble.

8) Decision algorithm is to return the word with the lowest merged frequency greater than 600.

## Notes
The logic behind the decision algorithm is that a phrase with a merged frequency lower than this is likely composed of stop words (e.g. "and", "or", "but"). 600 happens to be the lowest whole hundred number bound that could have potentially returned the result for puzzle 5, "vested(0) interest(652)". This threshold could be deteremined through emperical methods. It should be noted that the freq_dict is pretty lacking as a language model, we could improve our predictions by including more frequency data or by leveraging an actual language model.

Since there are a number of solutions with words that have frequency in the freq_dict as 0, we could only achieve these by doing very expensive cross-join operations. Ideally we would be able to filter out words with freq=0 and freq > some threshold.

The actual solutions for the puzzles are:

1 : "jab well done",

2 : "bad hair day",

3 : "rash decision",

4 : "addition",

5 : "vested interest"

Running on 16GB of RAM and utilizing 4-cores on the local machine the current runtimes and solutions were:

1 : ~16min, "job waly need"

2 : ~10min, "day mabi dec"

3 : ~1.8min, "head incisors"

4 : ~0.5min, "addition"

5 : ~6.3min, "veldts interest"

Only jumble 4 is solved correctly. If we print additional candidates, we see that the answers for jumble 3 and 5 are only a few positions off from the best given answer. This is because of the increased search space for candidate characters for the final jumble, the increased number of words that need to be predicted, and the large number of 3-gram and 4-gram words compared to the 7-gram and 8-gram words in the last 3 puzzles.
