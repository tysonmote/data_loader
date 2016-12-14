# data_loader

data_loader is a simple exercise in loading arbitrary columnar data given CSV
spec files.

# Building / running

```
% go build
% ./data_loader
2016/12/14 10:21:24 Loading spec: specs/testformat1.csv
2016/12/14 10:21:24 Creating table "testformat1"
2016/12/14 10:21:24 Opening data file: data/testformat1_2015-06-28.txt
% sqlite3 -column -cmd "select * from testformat1" data.db
Foonyor     1           1
Barzane     0           -12
Quuxitude   1           103
```
