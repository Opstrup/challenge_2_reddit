from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import itertools

class CommonAuthors(MRJob):



    def mapper(self,_,line):
        json_string = json.loads(line)
        yield json_string['author'], json_string['subreddit_id']

    def reducer_remove_deleted_authors(self,author,subreddit_id):
        if author != "[deleted]":
            yield list(set(subreddit_id)),author    

    def mapper_generating_pairs(self,subreddit_id,author):
        if len(subreddit_id) > 1 and len(subreddit_id) <= 2:
            yield None,(len(list(author)),subreddit_id)
        
        elif len(subreddit_id) >2:
            for pair in itertools.combinations(subreddit_id,2):
                yield None,(len(list(author)),pair)

    def reducer_top10(self, _, commonAuthors):
        sorted_list = sorted(commonAuthors, key=lambda tup: tup[0], reverse = True)
        yield "10 pairs with most authors in common: ", sorted_list[:10]      

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_remove_deleted_authors),
            MRStep(mapper=self.mapper_generating_pairs,
                   reducer=self.reducer_top10)
        ]

if __name__ == '__main__':
    CommonAuthors.run()