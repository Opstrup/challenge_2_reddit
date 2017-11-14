from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import itertools
import operator

class LargestVocabulary(MRJob):

    def mapper_get_data(self, _, line):
        json_string = json.loads(line)
        yield (json_string['subreddit_id'], json_string['subreddit']), json_string['parent_id']

    def mapper_yield_funny_subreddits(self, subr, value):
        (subr_id, subr_name) = subr
        if subr_id == 't5_2qh33':
            yield subr, value

    def reducer_count_number_of_comments(self, subr, value):
        if value[:2] == 't3':
            yield subr, 0
        else:
            yield subr, 1

    def reducer_calculate_average_depth(self, subr, values):
        list_values = list(values)
        print(subr, list_values)
        if list_values.count(0) is 0:
            yield subr, 0
        else:
            yield subr, sum(list_values) / list_values.count(0)

    def reducer_get_highest_ten(self, _, unique_count):
        sorted_list = sorted(unique_count, key=lambda count: count[1], reverse = True)
        yield "10 highest average depth", sorted_list[:10]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_data),
            MRStep(mapper=self.reducer_count_number_of_comments,
                   reducer=self.reducer_calculate_average_depth)
        ]

if __name__ == '__main__':
    LargestVocabulary.run()