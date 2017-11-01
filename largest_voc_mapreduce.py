from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class LargestVocabulary(MRJob):

    def mapper_node_connections(self, _, line):
        json_string = json.loads(line)
        unique_count = len(set(json_string['body'].split()))
        yield json_string['subreddit_id'], unique_count

    def reducer_sum_connections(self, subr_id, unique_counts):
        yield None, (subr_id, sum(unique_counts))

    def reducer_get_highest_ten(self, _, unique_count):
        sorted_list = sorted(unique_count, key=lambda count: count[1], reverse = True)
        yield "10 biggest vocabulary", sorted_list[:10]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_node_connections,
                   reducer=self.reducer_sum_connections),
            MRStep(reducer=self.reducer_get_highest_ten)
        ]

if __name__ == '__main__':
    LargestVocabulary.run()