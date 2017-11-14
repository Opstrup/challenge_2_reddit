from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import itertools
import operator

class LargestVocabulary(MRJob):

    def mapper_get_data(self, _, line):
        json_string = json.loads(line)
        yield (json_string['subreddit_id'], json_string['subreddit']), json_string['parent_id']

    def mapper_count_number_of_comments(self, subr, value):
        if value[:2] == 't3':
            yield subr, 0
        else:
            yield subr, 1

    def reducer_calculate_average_depth(self, subr, values):
        list_values = list(values)
        if list_values.count(0) is 0:
            yield subr, 0
        else:
            yield subr, sum(list_values) / list_values.count(0)

    def mapper_get_highest_ten(self, subr, average_depth):
        yield None, (subr, average_depth)

    def reducer_get_highest_ten(self, _, subr_depth):
        sorted_list = sorted(subr_depth, key=lambda tup: tup[1], reverse = True)
        yield "Highest ten: ", sorted_list[:10]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_data),
            MRStep(mapper=self.mapper_count_number_of_comments,
                   reducer=self.reducer_calculate_average_depth),
            MRStep(mapper=self.mapper_get_highest_ten,
                   reducer=self.reducer_get_highest_ten)
        ]

if __name__ == '__main__':
    LargestVocabulary.run()