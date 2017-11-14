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

    def reducer_get_highest_ten(self, subr, average_depth):
        res_list = []
        if len(res_list) < 10:
            res_list.append(average_depth)
            sorted(res_list, reverse = True)
            yield subr, sorted(res_list, reverse = True)
        elif res_list[9] < average_depth:
            print('res list', res_list)
            res_list[9] = average_depth
            yield subr, sorted(res_list, reverse = True)
        

        # yield "10 highest average depth", res_list
        # sorted_list = sorted(list(average_depth), reverse = True)
        # yield "10 highest average depth", sorted_list[:10]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_data),
            MRStep(mapper=self.mapper_count_number_of_comments,
                   reducer=self.reducer_calculate_average_depth),
            MRStep(mapper=self.reducer_get_highest_ten)
        ]

if __name__ == '__main__':
    LargestVocabulary.run()