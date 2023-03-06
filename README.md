# Spark Data Processing

Given:  
A table with three columns: `Integer` Number, `String` Company, `Integer` Value.

- Number column is a sequence:
  1,2,3 ...
  N - where N is the size of the table.
- Company column might contain only 1 unique string like "APPLE" or many different
  strings.
- Value column contains only positive integer numbers.

Find: The previous Number and Value for Company where Value is more than 1000. If the previous does not exist - return
0, 0.

Input: input/input-X.csv with Number,Company,Integer columns.  
X is a sequence - 1,2,3...X.  
Each file contains a globally
sorted partition of the table by Number.  
Each partition contains up to 10 million rows.  
The total number of rows is less
than 10 billions.

### Example Input

```input-1.csv:
1,APPLE,2000
2,GOOGLE,10
3,MICROSOFT,5000
4,APPLE,100
```

```input-2.csv:
5,GOOGLE,2000
6,MICROSOFT,3000
7,GOOGLE,100
8,GOOGLE,200
```

### Example Output

```output-1.csv:
1,APPLE,2000,0,0
2,GOOGLE,10,0,0
3,MICROSOFT,5000,0,0
```

```output-2.csv
4,APPLE,100,1,2000
5,GOOGLE,2000,0,0
6,MICROSOFT,3000,3,5000
```

```output-3.csv:
7,GOOGLE,100,5,2000
8,GOOGLE,200,5,2000
```

## My Solution

- I will be referring to the last two columns of the output as the "answer" for the entry.
  In the output entry (7, GOOGLE, 100, 5, 2000), the (5, 2000) portion is the "answer" for entry 7.
- Note that each company is independent. By grouping by company, we can now focus on solving the problem for entries of
  a single company.
- Observations:
    1. If the entry `i` has value > 1000, we know that `i` is the answer for the entry `i+1`.
    2. The answer for entry `i+1` is always either:
        - The entry `i`, if its value is 1000
        - Or the answer for entry `i`

These observations allow us to solve the problem in "two steps".

1. Iterate over the sorted-by-id entries, and use Observation i.
    - Now we have some answers already calculated, but many of them are still 0.
2. Iterate over these previous results, in order, and use Observation ii. to "propagate" the answers.

We have correctly computed the answer for all entries of a single company, and now we can merge the RDD
again.

## Notes

- My output format is different than desired. By default the RDD/DataFrame saves the output into multiple files with a
  set naming convention.
  I did not find any good way of changing that naming convention. I thought of:
    1. Rename all output files in place.
       (But maybe Spark uses the naming conventions for something and we should not change it?)
    2. Copy all the output files and rename the copies.
       (Bad because we would be using 2x the amount of storage to save the files, which can be unfeasible)
- It would be quite easy if we could "keep state" between the iterations of `map`, but I don't see how that would work
  given the parallel nature of RDDs.
- I think there must be some simpler way of solving the problem? This requires using a sliding window, and iterating two
  times, while the imperative version would be quite straight-forward.
- I tried using accumulators, but could not get it working.
- I am not sure what could break when dealing with a lot of data. There are some tests under `test` folder, but I did
  not know
  how to test that aspect.