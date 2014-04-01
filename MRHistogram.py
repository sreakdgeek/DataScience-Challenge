
__author__ = 'srikanthvidapanakal'

from mrjob.job import MRJob
import chardet

cutoff = 5

class MRHistogram(MRJob):

    def mapper(self, _, line):
        like_arr = line.strip().split(",")
        encoding = {}

        for like in like_arr[1:]:
            like = like[1:-1]
            like = like.strip()
            encoding = chardet.detect(like)
            #print encoding
            if len(like) != 1:  # Eliminate single-character likes

                encoding_scheme = encoding['encoding']

                if encoding_scheme is None:
                    encoding_scheme = 'ISO-8859-1'

                try:
                    like = like.decode(encoding_scheme)
                except UnicodeDecodeError:
                    like = like.decode('ISO-8859-1')

                yield (like, like_arr[0])

    def reducer(self, like, like_ids):

        like_list_ids = list(like_ids)
        like_count = len(like_list_ids)

        if len(like_list_ids) >= cutoff:
            like = like.encode('UTF-8')
            #print type(like_list)
            yield "%s, %s" % (like, like_count), like_list_ids

# Main
if __name__ == '__main__':
    MRHistogram.run()