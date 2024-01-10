# What is in here? 

This folder contains scripts for downloading and processing PANDA-DAWN datasets from nodes: 

- download_radenv.py : download the panda-radenv-stream bags for the queried time range
- read_data.py : script to loop over bags, read data, and plot time series
- utils.py : some helper classes to read/plot the data

The .png files included in this directory show an example output of the read_data.py scripts ran over a set of five bags uploaded by node W022.
