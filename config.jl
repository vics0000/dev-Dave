# config.jl
# using Distributed # 0.7 code

#const ROOTDIR = "/home/dave/Races/"
const ROOTDIR = "/Users/darylgregory/Harness/vst-prod/"
@everywhere ENV["TMDIR"] = ROOTDIR * "trackmaster/"

# Unit Test
# data directories set by each test

# Dev config
#@everywhere ENV["DATADIR"] = ROOTDIR * "data/"
#@everywhere ENV["TMDATADIR"] = ROOTDIR * "data/TrackMaster/"
#@everywhere ENV["TMDIR"] = ROOTDIR * "dev/trackmaster/"
