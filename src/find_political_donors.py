import argparse
import heapq
import collections
import csv
import random
import sys

from datetime import datetime

"""
Algorithm description
======================

Running median
----------------

To calculate the running median, for each group of (recipient, zipcode) pair,
we maintain two heaps of about equal size. [smaller_heap] is a max-heap that
keeps the first half of all the amounts we have seen so far.  [bigger_heap] is
a min-heap that contains the rest.  Two important invariants are:

  1) values in [smaller_heap] are less than or equal to values in [bigger_heap]
  2) the size of the two heaps are at most offset by one, i.e.,
       0 <= len(bigger_heap) - len(smaller_heap) <= 1

By this definition, at any given time, the current median is either the top of
[bigger_heap], or the average of two top elements from both heaps.

Since heap operations have O(log n) complexity, hash table operations has O(1)
complexity, the total complexity of the algorithm is O(n log k) where n is the
total number of items, and k is the average number of items for any given
recipient and zipcode.  If k is very small compared to n, this is roughly
equivant to O(n). In the worst case (when the input contains a single recipient
and a single zipcode), the complexity is O(n log n).


Final median
----------------
To calculate the median of each (recipient, date) pair, assuming all data has
been read into the memory, we can simply apply the quick-select algorithm to
each group of data.  The complexity of the algorithm is always O(n).

"""

# Names of all columns in the input file, copied from
# http://classic.fec.gov/finance/disclosure/metadata/indiv_header_file.csv
ALL_COLUMNS = 'CMTE_ID,AMNDT_IND,RPT_TP,TRANSACTION_PGI,IMAGE_NUM,TRANSACTION_TP,ENTITY_TP,NAME,CITY,STATE,ZIP_CODE,EMPLOYER,OCCUPATION,TRANSACTION_DT,TRANSACTION_AMT,OTHER_ID,TRAN_ID,FILE_NUM,MEMO_CD,MEMO_TEXT,SUB_ID'.split(',')

# Date format in input / output file
DATE_FORMAT = '%m%d%Y'

# A named tuple containing only columns of interest
ParsedRow = collections.namedtuple('ParsedRow', ['recipient', 'zipcode', 'date', 'amount'])


# Bookkeeping states for running median
class RunningState:
    def __init__(self):
        self.count = 0
        self.total = 0
        self.median = 0
        self.smaller_heap = []  # max heap (use negate)
        self.bigger_heap = []   # min heap

    def update_median(self, v):
        # insert the new element into one of the two heap and keep them balanced
        v = heapq.heappushpop(self.bigger_heap, v)
        v = -heapq.heappushpop(self.smaller_heap, -v)
        if len(self.bigger_heap) <= len(self.smaller_heap):
            heapq.heappush(self.bigger_heap, v)
        else:
            heapq.heappush(self.smaller_heap, -v)

        # find the current median by looking at the top of the two heaps
        if len(self.bigger_heap) > len(self.smaller_heap):
            return self.bigger_heap[0]
        else:
            return int(round(float(self.bigger_heap[0] - self.smaller_heap[0]) / 2))

    def update(self, amount):
        self.count += 1
        self.total += amount
        self.median = self.update_median(amount)
        return self


class ZipcodeHandler:
    def __init__(self, outfile):
        self.data = collections.defaultdict(RunningState)
        self.outfile = outfile

    def update(self, row):
        # calucate the running median, count and total, skip invalid rows
        if row.zipcode is not None:
            st = self.data[(row.recipient, row.zipcode)].update(row.amount)
            cols = [row.recipient, row.zipcode, str(st.median), str(st.count), str(st.total)]
            print >>self.outfile, '|'.join(cols)

    def finalize(self):
        pass


class DateHandler:
    def __init__(self, outfile):
        self.data = collections.defaultdict(list)
        self.outfile = outfile

    def update(self, row):
        # just group the input row by recipient and date, skip invalid rows
        if row.date is not None:
            self.data[(row.recipient, row.date)].append(row.amount)

    def quick_select(self, values, index):
        # choose a random pivot and partition values into 3 groups
        pivot = random.choice(values)
        smaller, bigger, equals = [], [], 0
        for v in values:
            if v < pivot:
                smaller.append(v)
            elif v > pivot:
                bigger.append(v)
            else:
                equals += 1

        # recursively call quick_select based on the size of each group
        if len(smaller) > index:
            return self.quick_select(smaller, index)
        elif len(smaller) + equals > index:
            return pivot
        return self.quick_select(bigger, index - len(smaller) - equals)

    def median(self, values):
        nr = len(values)
        if nr % 2 == 1:
            # odd number of items, pick the middle one
            return self.quick_select(values, nr / 2)
        else:
            # even number of items, return the average of the middle two
            v1 = self.quick_select(values, (nr - 1) / 2)
            v2 = self.quick_select(values, (nr + 1) / 2)
            return int(round(float(v1 + v2) / 2))

    def finalize(self):
        # calculate the final sum, count and median in one pass
        for key in sorted(self.data.keys()):
            amounts = self.data[key]
            recipient = key[0]
            date = key[1].strftime(DATE_FORMAT)
            median = str(self.median(amounts))
            count = str(len(amounts))
            total = str(sum(amounts))
            print >>self.outfile, '|'.join([recipient, date, median, count, total])


class DonorPipeline:
    def __init__(self, handlers):
        self.handlers = handlers

    # Return None for invalid zip code and retain only the first 5 digits
    def sanitize_zipcode(self, zipcode):
        if len(zipcode) < 5 or not zipcode.isdigit():
            return None
        return zipcode[:5]

    # Return a parse date object and None if the argument is not a valid date string
    def sanitize_date(self, date_str):
        try:
            return datetime.strptime(date_str, DATE_FORMAT)
        except ValueError:
            return None

    def process(self, infile):
        csv.register_dialect('pipes', delimiter='|', quoting=csv.QUOTE_NONE)
        reader = csv.DictReader(infile, fieldnames=ALL_COLUMNS, dialect='pipes')
        for line in reader:
            # skip invalid data (requirement 1 and 5)
            if line['OTHER_ID'] or not line['CMTE_ID'] or not line['TRANSACTION_AMT']:
                continue
            # pick columns of interest and sanitize input data
            row = ParsedRow(
                    recipient = line['CMTE_ID'],
                    zipcode = self.sanitize_zipcode(line['ZIP_CODE']),
                    date = self.sanitize_date(line['TRANSACTION_DT']),
                    amount = int(line['TRANSACTION_AMT']))
            # for each row, run through each handlers
            for handler in self.handlers:
                handler.update(row)

        # post-processing
        for handler in self.handlers:
            handler.finalize()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', type=argparse.FileType(),
            default=sys.stdin)
    parser.add_argument('by_zip_output_file', type=argparse.FileType('w'),
            default=sys.stdout)
    parser.add_argument('by_date_output_file', type=argparse.FileType('w'),
            default=sys.stdout)
    return parser.parse_args()


def main():
    args = parse_args()

    # initialize pipeline by adding two handlers
    zipcode_handler = ZipcodeHandler(args.by_zip_output_file)
    date_handler = DateHandler(args.by_date_output_file)
    pipeline = DonorPipeline([zipcode_handler, date_handler])

    # process input file using the pipeline
    pipeline.process(args.input_file)

    # close output files
    args.by_zip_output_file.close()
    args.by_date_output_file.close()


if __name__ == '__main__':
    main()
