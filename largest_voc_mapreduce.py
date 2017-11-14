from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class LargestVocabulary(MRJob):

    def mapper_get_unique_words(self, _, line):
        symbols = ['\n','`','~','!','@','#','$','%','^','&','*','(',')','_','-','+','=','{','[',']','}','|','\\',':',';','"',"'",'<','>','.','?','/',',', '\r']
        json_string = json.loads(line)
        lower_string = json_string['body'].lower()

        for sym in symbols:
            lower_string = lower_string.replace(sym, "")

        unique_count = len(set(lower_string.split()))
        yield (json_string['subreddit_id'], json_string['subreddit']), unique_count

    def reducer_sum_unique_words(self, subr, unique_counts):
        yield None, (subr, sum(unique_counts))

    def reducer_get_highest_ten(self, _, unique_count):
        sorted_list = sorted(unique_count, key=lambda count: count[1], reverse = True)
        yield "10 biggest vocabulary", sorted_list[:10]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_unique_words,
                   reducer=self.reducer_sum_unique_words),
            MRStep(reducer=self.reducer_get_highest_ten)
        ]

if __name__ == '__main__':
    LargestVocabulary.run()