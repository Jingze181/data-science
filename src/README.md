How to run
=====================

The project uses python 2.  There's no external dependencies.
Simply run with:

  $ ./run.sh


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
