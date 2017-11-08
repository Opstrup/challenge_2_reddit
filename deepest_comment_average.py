from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class LargestVocabulary(MRJob):

    def mapper_get_unique_words(self, _, line):
        json_string = json.loads(line)
        yield json_string['subreddit_id'], (json_string['parent_id'], json_string['name'])

    def reducer_sum_unique_words(self, subr_id, unique_counts):
        yield None, (subr_id, sum(unique_counts))

    def reducer_get_highest_ten(self, _, unique_count):
        sorted_list = sorted(unique_count, key=lambda count: count[1], reverse = True)
        yield "10 biggest vocabulary", sorted_list[:10]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_unique_words)
                #    reducer=self.reducer_sum_unique_words),
            # MRStep(reducer=self.reducer_get_highest_ten)
        ]

if __name__ == '__main__':
    LargestVocabulary.run()