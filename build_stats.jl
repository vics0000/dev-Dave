# build_stats.jl

@everywhere ENV["DATADIR"] = "/home/dave/Races/data/"
@everywhere ENV["TMDIR"] = "/home/dave/Races/dev/trackmaster/"

@everywhere include(ENV["TMDIR"]*"alt_type.jl")
@everywhere include(ENV["TMDIR"]*"utility_fns.jl")
@everywhere include(ENV["TMDIR"]*"daily_stats_fns.jl")

#################################################################

@everywhere function build_stats!(date::Int64, races::DataFrame, r_inds, racelines::DataFrame, indexes, stats::StatsTables)
    println("The date is $date")
#	if date <= stats.current return end
	day_dps = get_date_dps(indexes["dates"], date, date, true)
println("Before build_card_dets")
@time	roster = build_card_dets(date, racelines, day_dps)
	e_dps = indexes["entries"]["T"]
	day_dps = fast_intersect(day_dps, e_dps)
println("Before driver_stats")
@time	driver_stats(date, roster, stats)
println("Before trainer_stats")
@time	trainer_stats(date, roster, stats)
println("Before horse_stats")
@time	horse_stats(date, roster, stats)
println("Before track_stats")
@time	track_stats(date, roster, stats) 
println("Before track_ratings")
@time	track_ratings(date, roster, stats)
println("Before check_stats")
@time	check_stats(races, r_inds, racelines, date, day_dps, indexes, stats, roster)
#fred
end

@everywhere function process_races(sd::Int64, ed::Int64)
    racelines, races = load_all_post_lines()
#	init_BVC_races()
	r_inds = index_races_by_date(races)
	indexes = index_races(racelines)
#	old_dps = get_date_dps(indexes["dates"], sd)
	tables = load_stats_tables()
#    racedates = CSV.read(ENV["DATADIR"]*"races.csv")
#	 dates = unique(racedates[:date])
	dates = unique(races[:Date])

    for date in dates
        if date < sd continue
        elseif date > ed break end
#		old_dps = build_stats!(date, races, BVC_race_indexes, racelines, indexes, old_dps, tables)
		build_stats!(date, races, r_inds, racelines, indexes, tables)
    end
end

#################################################################
# Averages program

#SD = parse(Int64, ARGS[1])
#ED = parse(Int64, ARGS[2])
SD = 20150601
#ED = 20161027
#ED = 20160117
ED = 20150601

process_races(SD, ED)
