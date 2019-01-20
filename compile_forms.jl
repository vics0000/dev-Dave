# compile_forms.jl

@everywhere ENV["DATADIR"] = "/home/dave/Races/data/"
@everywhere ENV["TMDIR"] = "/home/dave/Races/dev/trackmaster/"

@everywhere include(ENV["TMDIR"]*"alt_type.jl")
@everywhere include(ENV["TMDIR"]*"utility_fns.jl")

#################################################################

@everywhere function compile_form(date::Int64, races::DataFrame, fname::String)
	dir = ENV["DATADIR"] * "dailies/$date/"
	println("The date is $date")
	forms = CSV.read(dir*"FormsPP.csv")
	println("Done reading ", dir*"FormsPP.csv")
	lines = races[find(races[:Date] .== date), :]
	println("adding forms")
	racelines = join(lines, forms, on=[:Rnum, :Hnum, :Post])
	append_forms(fname, racelines)
end

@everywhere function process_races(sd::Int64, ed::Int64)
	dir = ENV["DATADIR"]
    println("Loading racelines")
	races = CSV.read(dir*"racelines.csv")
	indexes = find(races[:PP] .== "F")
	deleterows!(races, indexes)
    lines = CSV.read(dir*"lines.csv"; types=Dict("Head"=>String))
	lines[:Entry] = map(x->parse_entry(x), lines[:Head])
    println("Joining racelines")
    racelines = join(races, lines, on=[:Rnum])
    racedates = CSV.read(ENV["DATADIR"]*"races.csv")
    dates = unique(racedates[:date])
    sort!(dates)
	fname = dir*"FormsPP_"*"$sd"*"_"*"$ed"*".csv"

    for date in dates
        if date < sd continue
        elseif date > ed break end
        compile_form(date, racelines, fname)
    end
end

#################################################################
# Forms program

SD = 20150601
ED = 20150601

process_races(SD, ED)
