from mrjob.job import MRJob
from mrjob.step import MRStep

class PatentCount(MRJob):

    def mapper(self, _, line):
        # # really this is all we need but to avoid parsing logfiles we do the whole computation
        # self.increment_counter('patent', 'count')
        yield "patents", 1

    def reducer(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [
            MRStep( mapper   = self.mapper,
                    combiner = self.reducer,
                    reducer  = self.reducer )
            ]
    
        
if __name__ == '__main__':
    PatentCount.run()
