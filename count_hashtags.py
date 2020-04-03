from mrjob.job import MRJob
import re


class CountHashtags(MRJob):

    def mapper(self, _, line):
        words = line.split()

        for word in words:
            if word.startswith("#"):
                word = re.sub("[^a-zA-Z# ]", "", word)
                yield (word.lower(), 1)


    def reducer(self, word, counts):
        yield (word, sum(counts))


if __name__ == '__main__':
    CountHashtags.run()
