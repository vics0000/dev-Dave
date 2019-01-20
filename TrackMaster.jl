#!/home/dave/julia-d55cadc350/bin/julia
using JLD
#using Distributed # 0.7 code

@everywhere include("config.jl")
@everywhere include(ENV["TMDIR"]*"alt_type.jl")
@everywhere include(ENV["TMDIR"]*"alt_parse.jl")
@everywhere include(ENV["TMDIR"]*"alt_pp_parse.jl")
@everywhere include(ENV["TMDIR"]*"utility_fns.jl")
@everywhere include(ENV["TMDIR"]*"do_results.jl")

# Define a callback function to receive data
@everywhere function cb_chart(chart::Chart, raceData::Tables)
    println(chart.race_date, " ", chart.trackdata.track, " ", chart.track.name)
    cb_results_obj!(chart, raceData)
end


# Defined in trackmaster/alt_type.jl
# type Tables
#     source::String
#     tracks::Dict{String, Track}
#     new_race::String
#     horses::Dict{String, horseDetails}
#     trainers::Dict{String, String}
#     drivers::Dict{String, String}
#     lines_by_date::Dict{Int64, Lines}
# end

# NOTE: raceData was a global. Now that it's in functions,
# will the code break?

"""
	parsePPSandResultsFiles()

	parse all xml files in the trackmaster/pps folder
	then parse all xml files in the trackmaster/results folder
"""
function parsePPSandResultsFiles()
	parsePPSFiles()
	parseResultsFiles()
end


"""
	parsePpsFiles()

	Loop thru all the xml files in the trackmaster/pps folder,
	parse them, and save to dailies directory and root of data dir.
"""
function parsePPSFiles()
	raceData = Tables()
	raceData.source = "pps"
	dir = ENV["TMDATADIR"]*"pps/"
	for f in readdir(dir)
		if (endswith(f, ".xml"))
	    	pp_parse_file(raceData, dir, f)
	  		save_tables(raceData)
		end
	end
end

function parseResultsFiles()
	# NOTE: May need to inherit table from parseAllPpsFiles()
	raceData = Tables()
	raceData.source = "results"
	dir = ENV["TMDATADIR"]*"results/"
	for f in readdir(dir)
		if (contains(f, ".xml"))
	    	alt_parse_file(raceData, dir*f)
	        save_tables(raceData)
		end
	end
end

function testpps1file()
	raceData = Tables()
	println("testpps1file BEGIN")
	raceData.source = "pps"
	pp_parse_file(raceData, ENV["TMDATADIR"]*"pps/", "yrx20180713hmpXML.xml")
	save_tables(raceData)
end

# raceData.source = "results"
# alt_parse_file(raceData, ENV["TMDATADIR"]*"results/yrx20180713.xml")
# save_tables(raceData)
