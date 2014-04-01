__author__ = 'srikanthvidapanakal'

from pylab import *
import matplotlib.pyplot as plt
import codecs
import csv


def parse_likes_file(likes_file):
    in_fp = codecs.open(likes_file, "r", encoding='ISO-8859-1')
    my_histogram = {}

    for line in in_fp.readlines():
        like_arr = line.strip().split(",")
        for item in like_arr[1:]:
            item = item[1:-1]
            if item in my_histogram:
                my_histogram[item] += 1
            else:
                my_histogram[item] = 1

    return my_histogram

# Main

my_likes_file = "/Users/srikanthvidapanakal/Documents/likes.csv"
my_histogram_file = "/Users/srikanthvidapanakal/Documents/histogram.csv"
writer = csv.writer(open(my_histogram_file, 'w', encoding='ISO-8859-1'))

like_histogram = parse_likes_file(my_likes_file)

for like in sorted(like_histogram, key=like_histogram.get, reverse=True):
    writer.writerow([bytes.decode(like.encode('ISO-8859-1'), 'ISO-8859-1'), like_histogram[like]])
