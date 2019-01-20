# test.jl
include("../../../config.jl")
const SCENARIODIR = ROOTDIR * "tests/trackmaster/"
const TESTDIR = SCENARIODIR * "/test-02/"
ENV["DATADIR"] = TESTDIR * "run-data/"
ENV["TMDATADIR"] = ENV["DATADIR"] * "trackmaster/"

include("../../testutils.jl")

include(ROOTDIR * "TrackMaster.jl")

# parsePPSandResultsFiles()
# parsePPSFiles()
# parseResultsFiles()

function testresults()
    setupData(TESTDIR)
    parseResultsFiles()
end
