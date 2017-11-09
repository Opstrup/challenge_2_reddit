from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import itertools

class CommonAuthors(MRJob):

    commonAuthors = []

    def mapper(self,_,line):
        json_string = json.loads(line)
        yield json_string['author'], json_string['subreddit_id']

    def reducer(self,author,subreddit_id):
        if author != "[deleted]":
            yield list(set(subreddit_id)),author    

    def mapper2(self,subreddit_id,author):
        if len(subreddit_id) > 1 and len(subreddit_id) <= 2:
            yield subreddit_id,author
        
        elif len(subreddit_id) >2:
            for pair in itertools.combinations(subreddit_id,2):
                yield pair,author

    def reducer2(self,subreddit_id,author):
        yield len(list(author)),subreddit_id

    def mapper3(self,author,subreddit_id):
        if author > 1:
            yield author,subreddit_id     

        #for pair in itertools.permutations(my_list):
        #    yield pair,None
        #    for (index,subID) in enumerate(subreddit_id):
        #        if index % 2 == 0:
        #            yield (subreddit_id[index-1],subreddit_id[index-1]),author


        #elif len(subreddit_id) < 3 and len(subreddit_id) % 2 = 1:    


    # def mapper(self, _, line):
    #     json_string = json.loads(line)
    #     yield json_string['author'], json_string['subreddit_id']

    # def mapper2(self, subreddit_id,author):
    #     if len(subreddit_id) > 1 and author != '[deleted]':
    #         for ID in subreddit_id:
    #             yield ID,1
    # def mapper3(self,_,authors):
    #     yield None,list(authors)            
             
    # def reducer_author_id(self, author, subreddit_id):
    #     yield list(set(subreddit_id)),author

    # def reducer_author_pairs(self, ID, author):
    #     yield ID, sum(author)

    # def reducer_get_highest_ten(self, _, author):
    #    print(list(authors))
        #sorted_list = sorted(author)
        #yield "10 biggest vocabulary", sorted_list[:10]

        #yield "10 biggest vocabulary", sorted_list[:10]        

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper2,
                    reducer=self.reducer2),
            MRStep(mapper=self.mapper3),
                    #reducer=self.reducer3)
            #       reducer=self.reducer_author_pairs),
            #MRStep(mapper=self.mapper3,
            #       reducer=self.reducer_get_highest_ten)
        ]

if __name__ == '__main__':
    CommonAuthors.run()