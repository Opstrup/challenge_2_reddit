from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import itertools
import operator

class LargestVocabulary(MRJob):

    def mapper_get_unique_words(self, _, line):
        json_string = json.loads(line)
        yield (json_string['subreddit_id'], json_string['subreddit']), json_string['parent_id']

    def combiner_grp_all_comments(self, subr, value):
        yield (subr, value)

    def mapper_yield_funny_subreddits(self, subr, value):
        (subr_id, subr_name) = subr
        if subr_id == 't5_2qh33':
            yield subr, value

    def reducer_sum_unique_words(self, subr, unique_counts):
        yield None, (subr, sum(unique_counts))

    def reducer_get_highest_ten(self, _, unique_count):
        sorted_list = sorted(unique_count, key=lambda count: count[1], reverse = True)
        yield "10 biggest vocabulary", sorted_list[:10]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_unique_words,
                   combiner=self.combiner_grp_all_comments)
            # MRStep(mapper=self.mapper_yield_funny_subreddits)
                #    reducer=self.reducer_sum_unique_words),
            # MRStep(reducer=self.reducer_get_highest_ten)
        ]

if __name__ == '__main__':
    LargestVocabulary.run()