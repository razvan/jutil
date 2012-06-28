#!/usr/bin/python

import multiprocessing
import string
import collections
import contextlib
import bz2
import sys
import re

import mr


def map_dfreq(filename):
	print >> sys.stderr, multiprocessing.current_process().name, 'reading', filename

	output = collections.defaultdict(lambda: 0)
	doc_terms = set()

	with contextlib.closing(bz2.BZ2File(filename, 'rb')) as f:

		for line in f:

			if "</doc>\n" == line:
				# push freqs to output
				for word in doc_terms:
					output[word] = output[word] + 1
				doc_terms = set()
			else:
				doc_terms = doc_terms | set([word.lower() for word in re.split('[,.:;|!?/"()\[\]\-\s\t]+', line) if word.decode("utf-8").isalpha()])

	if doc_terms:
		for word in doc_terms:
			output[word] = output[word] + 1

	return output.items()

def map_doc(filename):
	print >> sys.stderr, multiprocessing.current_process().name, 'reading', filename
	output = []
	doc_count = 1
	with contextlib.closing(bz2.BZ2File(filename, 'rb')) as f:
		for line in f:
			if "</doc>\n" == line:
				doc_count = doc_count + 1
		return [('doc_count', doc_count)]

def reduce_doc(item):
	name, doc_count = item
	return (name, sum(doc_count))


def file_to_words(filename):
	"""
	Read a file and return a sequence of (word, occurrences) values.
	"""
	print >> sys.stderr, multiprocessing.current_process().name, 'reading', filename
	output = collections.defaultdict(lambda: 0)
	with contextlib.closing(bz2.BZ2File(filename, 'rb')) as f:
		for line in f:
			for word in line.split():
				word = word.lower()
				if word.isalpha():
					output[word] = output[word] + 1
		return output.items()

def count_words(item):
	"""
	Convert the partitioned data for a word to a
	tuple containing the word and the number of occurrences.
	"""
	word, occurrences = item
	return (word, sum(occurrences))

if __name__ == '__main__':
	import glob

	if len(sys.argv) != 3:
		print >> sys.stderr, """Usage: wordcount.py '*.bz2' [doc|freq|dfreq]"""
		sys.exit()

	input_files = glob.glob(sys.argv[1])

	if 'doc' == sys.argv[2]:
		mapper = mr.SimpleMapReduce(map_doc, reduce_doc)
		doc_counts = mapper(input_files)
		s,c=doc_counts[0]
		print c

	if 'freq' == sys.argv[2]:
		mapper = mr.SimpleMapReduce(file_to_words, count_words)
		word_counts = mapper(input_files)

		for (word,count) in word_counts:
			print '%s\t%s' % (word, count)

	if 'dfreq' == sys.argv[2]:
		mapper = mr.SimpleMapReduce(map_dfreq, count_words)
		word_counts = mapper(input_files)

		for (word,count) in word_counts:
			print '%s\t%s' % (word, count)
