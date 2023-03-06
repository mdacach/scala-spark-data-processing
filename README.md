## Spark Data Processing

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