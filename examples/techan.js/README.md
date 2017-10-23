# Techan.js example

This example has borrowed one of Techan.js' examples with some slight modifications so it will read dataframes generated from ntfdl and exported as csv files with Pandas DataFrame.to_csv.

There is a file generated with ntfdl `stl.csv` for Statoil ASA in this example.

`index.html` contains the code to get Techan.js to render the data.


## Exporting trade data as csv for Techan.js

```
from ntfdl import multi

# We are going to get data for Statoil ASA (STL.OSE)
stl = multi(instrument='STL', exchange='OSE')

# Get intraday data from-to and resample to 15min
ohlcv = stl.get_ohlcv('20170801', '20171020', '15min')

# Pandas save as csv file
ohlcv.to_csv('stl.csv')
```

Using different name for file please edit `index.html`, specifically the `d3.csv("stl.csv", function(error, data) {..}` part.
