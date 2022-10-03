#!/usr/bin/env python
#
# Copyright 2009-2010 Gabriele Sales <gbrsales@gmail.com>
# Copyright 2010 Paolo Martini <paolo.cavei@gmail.com>

from collections import defaultdict
from itertools import groupby
from operator import itemgetter
from optparse import OptionParser
from sys import stdin, stderr
from vfork.io.util import safe_rstrip
from vfork.util import exit, format_usage, ignore_broken_pipe


def iter_records(fd, col, is_sorted):
	prev_key = None
	for line_idx, line in enumerate(fd):
		tokens = safe_rstrip(line).split('\t')
		if len(tokens) <= col:
			exit('Insufficient token number at line %d: %s' % (line_idx+1, line))

		value = tokens[col]
		del tokens[col]
		key = tuple(tokens)

		if is_sorted>0:
                        if is_sorted==1:
                            if prev_key is not None and key < prev_key:
                                    exit('Disorder found at line %d: %s' % (line_idx+1, line))
			    prev_key = key

		yield key, value

def group_sorted(records, preserve_order):
	for k, rs in groupby(records, itemgetter(0)):
		vs = [r[1] for r in rs]
		if not preserve_order:
			vs = set(vs)
		yield k, vs

def group_unsorted(records, preserve_order):
	if preserve_order:
		rs = defaultdict(list)
		add_record = lambda k,v: rs[k].append(v)
	else:
		rs = defaultdict(set)
		add_record = lambda k,v: rs[k].add(v)

	for k, v in records:
		add_record(k,v)

	return rs.iteritems()

def main():
	parser = OptionParser(usage=format_usage('''
		%prog COL <STDIN

		Perform the opposite operation of expandsets.
	'''))
	parser.add_option('-s', '--sorted-input', dest='sorted_input', action='store_true', default=False, help='the input is sorted on all columns except COL')
	parser.add_option('-S', '--assume-sorted-input', dest='assume_sorted_input', action='store_true', default=False, help='assume the input is sorted on all columns except COL, do not check')
	parser.add_option('-o', '--preserve-order', dest='preserve_order', action='store_true', default=False, help='preserve the order (and duplications) of collapsed elements with respect to the input')
	parser.add_option('-H', '--header', dest='header', action='store_true', default=False, help='the first row is an header')
	parser.add_option('-g', '--glue', dest='glue', type=str, default=';', help="glue string (default %default)")

	options, args = parser.parse_args()
	if len(args) != 1:
		exit('Unexpected argument number.')

	try:
		col = int(args[0]) - 1
	except ValueError:
		exit('Invalid column index: %s' % args[0])


        sorted_input=0
        if options.sorted_input:
            sorted_input=1
        if options.assume_sorted_input:
            sorted_input=2
        if options.header:
            header = stdin.readline()
            print header,

	records = iter_records(stdin, col, sorted_input)

	if sorted_input>0:
		group_strategy = group_sorted
	else:
		group_strategy = group_unsorted
	groups = group_strategy(records, options.preserve_order)

	for k, vs in groups:
		if options.preserve_order:
			vsj = options.glue.join(vs)
		else:
			vsj = options.glue.join(sorted(vs))
		o = list(k)
		o.insert(col, vsj)
		print '\t'.join(o)

if __name__ == '__main__':
	ignore_broken_pipe(main)

