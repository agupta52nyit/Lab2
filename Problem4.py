from mrjob.job import MRJob
import re

class Problem4(MRJob):

    def mapper(self, key, value):
        doc_num, text = value.split(": ", 1)
        words = re.compile(r"[\w']+").findall(text)
        for word in words:
            yield (word.lower(), doc_num)

    def reducer(self, key, value):
        doclist = list(set(value))
        yield (key, ", ".join(doclist))

if __name__ == '__main__':
    Problem4.run()
