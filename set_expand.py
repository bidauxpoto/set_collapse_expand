#!/usr/bin/env python
#
# Copyright 2010 Paolo Martini <paolo.cavei@gmail.com>

from optparse import OptionParser
from sys import stdin
from vfork.io.util import safe_rstrip
from vfork.util import exit, format_usage, ignore_broken_pipe

def parse_column_indexes(args):
	indexes = []
	for arg in args:
		try:
			value = int(arg)
			if arg <= 0:
				raise ValueError
		except ValueError:
			exit('Invalid column index: %s' % arg)

		indexes.append(value-1)

	indexes.sort()
	return indexes

def build_matrix(tokens, column_indexes, separator):
	matrix = [ None ] * len(column_indexes)
	for midx, cidx in enumerate(column_indexes):
		matrix[midx] = tokens[cidx].split(separator)
	return matrix

def iterate_matrix(matrix):
	counter = [0] * len(matrix)
	def increment():
		for i in xrange(0, len(matrix)):
			counter[i] += 1
			if counter[i] < len(matrix[i]):
				break
			else:
				counter[i] = 0
		else:
			raise StopIteration

	while True:
		yield [ matrix[i][counter[i]] for i in xrange(len(matrix)) ]
		increment()

def main():
	parser = OptionParser(usage=format_usage('''
                 %prog COLUMN... <TSV

                 Given a COLUMN with different entity separated by a specific string
                 the program expand them in multiple rows. 
                 If more that  1  COLUMN is specified, the cartesian of the two sets
                 is returned.

        '''))

	parser.add_option('-s', '--separator', type=str, dest='separator', default=";", help='indicate the separator for multiple values of the same key in input (default: %default)', metavar='SEPARATOR')
	parser.add_option('-p', '--pairs', dest='pairs', action="store_true", help='where exactly two columns to expand are indicated, do not return the cartesian of the two sets but assume that the sets on the two colunm have the same size and couple the i-nt element of the firt set with the i-nt element of the second set')
	parser.add_option('-t', '--tuples',dest='tuples', action="store_true", help='same as --pairs, kept for backward compatibility')
	options, args = parser.parse_args()

	if option$tuples:
		option$pairs = True

	if len(args) < 1:
		exit('Unexpected argument number.')

	column_indexes = parse_column_indexes(args)
	firstline=stdin.readline()
	n=len(firstline.rstrip().split('\t'))
	if options.reverse:
		rev=range(n)
		for i in column_indexes:
			rev.remove(i)
		column_indexes=rev
	lci=len(column_indexes)

	if options.pairs and len(column_indexes)!=2:
		exit("if --pairs is indicated then exactly 2 column to expand are required")

	for idx, line in enumerate(chain([firstline],stdin):
		tokens = line.rstrip('\r\n').split('\t')
		if len(tokens) <= column_indexes[-1]:
			exit('Insufficient column number at line %d.' % (idx+1,))

		matrix = build_matrix(tokens, column_indexes, options.separator)
		if not options.pairs: 
			for values in iterate_matrix(matrix):
				for value, cidx in zip(values, column_indexes):
						tokens[cidx] = value
				print '\t'.join(tokens)
		else:
			if(len(matrix[0])!=len(matrix[1])):
				exit("the two set to expand must have the same size when --pairs is indicated (%s - %s)" % (matrix[0], matrix[1]))

			for values in zip(matrix[0],matrix[1]):
				tokens[column_indexes[0]]=values[0]
				tokens[column_indexes[1]]=values[1]
				print '\t'.join(tokens)


if __name__ == '__main__':
	ignore_broken_pipe(main)
