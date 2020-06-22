

# Input: list of log files (perhaps only 2 for the prototype)
# Output: list of matching group items (likely, groups will in the form of log statements [dictionaries])
# Process: First, assume log files are in JSON format, or are easily convertible to dictionary.
#          Second, create knowledge bank of potential identifiers (feature extraction)
#          Next, preform clustering algorithms on the files. (consider using knowledge bank as parameter)
#          Finally, filter the clusters, check for identifier unification, match with other file groupings
# High level idea: determine identifiers after clustering?
def multi_log_match(logs: list):
    results = []
    # step 1: data validation

    # step 2: identifiers

    # step 3: clustering

    # step 4: matching

    return results