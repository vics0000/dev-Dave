# test.jl
include("../../../config.jl")
const SCENARIODIR = ROOTDIR * "tests/trackmaster/"
const TESTDIR = SCENARIODIR * "/test-01/"
ENV["DATADIR"] = TESTDIR * "run-data/"
ENV["TMDATADIR"] = ENV["DATADIR"] * "trackmaster/"

include("../../testutils.jl")

include(ROOTDIR * "TrackMaster.jl")

# parsePPSandResultsFiles()
# parsePPSFiles()
# parseResultsFiles()

function testpps()
    setupData(TESTDIR)
    parseResultsFiles()
    # Does dailies dir exist
    # Dailies has one subdirectory
    # Subdirectory is named 20180713
    #
    # does tattos dir exist
    # do races have 12 rows
end
