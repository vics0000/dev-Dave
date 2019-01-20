# build_fast_forms.jl

@everywhere ENV["DATADIR"] = "/home/dave/Races/data/"
@everywhere ENV["TMDIR"] = "/home/dave/Races/dev/trackmaster/"

@everywhere include(ENV["TMDIR"]*"alt_type.jl")
@everywhere include(ENV["TMDIR"]*"utility_fns.jl")
@everywhere include(ENV["TMDIR"]*"fast_form_fns.jl")
@everywhere include(ENV["TMDIR"]*"fast_facts.jl")

#################################################################
# forms functions fast

@everywhere function check_factors(fact::String, vals, horses, lines)
	if sum(isnan.(vals))!=0 || sum(isinf.(vals))!=0
		println(fact)
		println(vals)
		show(horses, true)
		println()
		show(lines, true)
		println()
		fred
	end
end

@everywhere function get_factors!(data::Dict, form::DataFrame, ff, horses::DataFrame, lines::DataFrame, inds::Dict, lines6::DataFrame)
	O_out_ff = [H1st,TC,DC,RC,HS]
	O_out = ["__ps_MLinR"]
	All_out = ["__ps_Hprob"]
	valid = "F"

	for fact in(ff[2])
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#here is the kludge I was mentioning -> the else (kludge) is needed when you load a single pair from load_fact_fns()
		if typeof(fact)==String
			factor = ff[2][1]
			stat = ff[2][2]
		else
			factor = fact[1]
			stat = fact[2]
		end
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		adjstats = ff[1](factor, stat, data, horses, lines, inds, lines6)
		form[Symbol(factor)] = nrow(horses)>nrow(adjstats)? add_back_zeros(adjstats, horses): adjstats[:Calc]
		check_factors(factor, form[Symbol(factor)], horses, lines)
		if factor in(All_out) continue end
		if factor in(O_out)
			form[Symbol(factor*"p")] = round.(stats_to_STD(form[Symbol(factor)]) ./ nrow(horses), 4)
		else
			form[Symbol(factor*"p")] = round.(form[Symbol(factor)] ./ nrow(horses), 4)
		end
		if factor in(O_out)||ff[1] in(O_out_ff)  continue end
		adjstats[:Calc] = STD_to_O(adjstats[:Calc])
		form[Symbol(factor*"O")] = nrow(horses)>nrow(adjstats)? add_back_zeros(adjstats, horses): adjstats[:Calc]
		form[Symbol(factor*"Op")] = round.(form[Symbol(factor*"O")] ./ nrow(horses), 4)
		if typeof(fact)==String break end
	end
end

@everywhere function process_race(data::Dict, horses::DataFrame, factors::Dict, linesall::DataFrame)
	past_x_ff = [H1st,HML,HmML,HCL,HMCL,HmCL]
#	past_t_ff = [HBTL]
past_t_ff = [HT,HTBx,TC,HS,DC,RC,HBTL]
	past_ff = [HBxTL,HBE,HSP,HSSP,BiasT,BiasE,BiasS,HFL,HmFL,PC,HOPS,HmOPS]
    form = horses[[:Rnum, :Hnum, :Post]]
	ldps = get_lines_dps(data, horses, 10)
	lines10 = data["cards"][ldps, :]
	lines10 = lines10[find(lines10[:Date] .< horses[1, :Date]), :]
	markup_valid!(lines10)
	lines10v = cleanup_lines(lines10, horses; ot=false)
	inds = index_races_by_col(lines10, :Hnum)
	markup_lines!(lines10, inds, horses, Symbol("Tnum"))
	indsV = index_races_by_col(lines10v, :Hnum)
	markup_lines!(lines10v, indsV, horses, Symbol("Tnum"))
	horses[:Index] = 0
	lines10[:Index] = 0
	lines10v[:Index] = 0

    for ff in(factors)
		if ff[1] != 99
			horses[:CR] = ff[1]
			lines10[:CR] = ff[1]
			lines10v[:CR] = ff[1]
			lines = get_lines(lines10, horses, ff[1])
			inds = index_races_by_col(lines, :Hnum)
			markup_changes!(lines, inds, horses, Symbol("Tnum"))
			linesV = get_lines(lines10v, horses, ff[1])
			indsV = index_races_by_col(linesV, :Hnum)
			markup_changes!(linesV, indsV, horses, Symbol("Tnum"))
			ct_lines = cleanup_lines(linesV, horses)
			ct_inds = index_races_by_col(ct_lines, :Hnum)
			cx_lines = cleanup_lines(lines, horses; ftype="not_times")
			cx_inds = index_races_by_col(cx_lines, :Hnum)
cx_lines = ct_lines
cx_inds = ct_inds
		else lines = lines10 end

		for fgroup in ff[2]
			horses[:Calc] = 0.0
			horses[:Check] = "F"
			if fgroup[1]==TC||fgroup[1]==HS
				markup_changes!(linesV, indsV, horses, Symbol("Tnum"))
				tdh_indexes!(data["indexes"]["trainers"], horses, Symbol("Tnum"))
				tdh_indexes!(data["indexes"]["trainers"], linesV, Symbol("Tnum"))
			elseif fgroup[1]==DC
				markup_changes!(linesV, indsV, horses, Symbol("Dnum"))
				tdh_indexes!(data["indexes"]["drivers"], horses, Symbol("Dnum"))
				tdh_indexes!(data["indexes"]["drivers"], linesV, Symbol("Dnum"))
			elseif fgroup[1]==RC
				markup_changes!(linesV, indsV, horses, Symbol("Track"))
				post_indexes!(data["indexes"]["posts"], horses)
				post_indexes!(data["indexes"]["posts"], linesV)
			elseif fgroup[1]==PC
				post_indexes!(data["indexes"]["posts"], horses)
				post_indexes!(data["indexes"]["posts"], ct_lines)
			elseif fgroup[1]==HT
				markup_changes!(linesV, indsV, horses, Symbol("Tnum"))
			elseif fgroup[1]==Driver
				tdh_indexes!(data["indexes"]["drivers"], horses, Symbol("Dnum"))
			elseif fgroup[1]==Trainer||fgroup[1]==H1st
				tdh_indexes!(data["indexes"]["trainers"], horses, Symbol("Tnum"))
			elseif fgroup[1]==Horse
				tdh_indexes!(data["indexes"]["horses"], horses, Symbol("Hnum"))
			elseif fgroup[1]==Post
				post_indexes!(data["indexes"]["posts"], horses)
			end
			if fgroup[1] in past_ff
				get_factors!(data, form, fgroup, horses, ct_lines, ct_inds, linesall)
			elseif fgroup[1] in past_x_ff
				get_factors!(data, form, fgroup, horses, cx_lines, cx_inds, linesall)
			elseif fgroup[1] in past_t_ff
				get_factors!(data, form, fgroup, horses, linesV, indsV, linesall)
			else
				get_factors!(data, form, fgroup, horses, lines, inds, linesall)
			end
		end
	end
    form
end

@everywhere function build_form(date::Int64, facts::Dict, t_sizes::DataFrame, races::DataFrame, indexes)
	dir = ENV["DATADIR"] * "dailies/$date/"
	println("The date is $date")
	day_dps = get_date_dps(indexes["dates"], date, date, true)
	e_dps = indexes["entries"]["T"]
	day_dps = fast_intersect(day_dps, e_dps)
	dps = get_date_dps(indexes["dates"], date)
	in_dps = find(races[:Post] .> 0)
	dps = fast_intersect(dps, in_dps)
	cards = build_cards(races, dps, day_dps, 6)
	data = get_race_day_data(date)
	data["indexes"]["cards"] = index_races_by_col(cards, :Hnum)
	cards[:Calc] = 0.0
	cards[:CR] = 0
	cards[:Stat] = ""
	cards[:Valid] = true
	cards[:Change] = false
    cards[:Off] = false
	cards[:TR] = map(x->get_track_rating(data["tracks"], data["indexes"]["tracks"], "$x"), cards[:Track])
    cards[:TS] = map(x->get_track_size(x, t_sizes), cards[:Track])
	cards[:e1] = get_track_effort(cards[:TS], cards[:Qpos], cards[:Qpark], cards[:VC])
	cards[:e2] = get_track_effort(cards[:TS], cards[:Hpos], cards[:Hpark], cards[:VC])
	cards[:e3] = get_track_effort(cards[:TS], cards[:Tpos], cards[:Tpark], cards[:VC])
	cards[:e4] = get_track_effort(cards[:TS], cards[:Spos], cards[:Spark], cards[:VC])
	data["cards"] = cards
	CSV.write(dir*"Cards.csv", cards)

linesall = cards[find(cards[:Date] .<date), :]
markup_valid!(linesall)
linesall = linesall[find(linesall[:Valid] .==true), :]

    day_dps = fast_intersect(find(data["cards"][:Date] .== date), find(data["cards"][:Post] .> 0))
    programs = data["cards"][day_dps, :]
#	tracks = sort!(unique(programs[:Track]))
	tracks = unique(programs[:Track])
    forms = []

    for track in tracks
#track = "BANG"
        println("Building $track")
        lines = programs[find(programs[:Track] .== track), :]
        Rnums = unique(lines[:Rnum])
        for rnum in Rnums
#rnum = 89796
            println(rnum)
            flush(STDOUT)
			horses = lines[find(lines[:Rnum] .== rnum), :]
      		form = process_race(data, horses, facts, linesall)
      		forms = isempty(forms)? form: append!(forms, form)
        end
    end
    if !isempty(forms)
        extreme = find_extreme_vals(forms, names(forms)[4:end], 4)
        if !isempty(extreme) println(extreme) end
        CSV.write(dir*"FormsPP.csv", forms)
    end
end

@everywhere function process_races(sd::Int64, ed::Int64)
	dir = ENV["DATADIR"]
    println("Loading racelines")

#	races = CSV.read(dir*"races_BVC.csv")
	races = CSV.read(dir*"racelines.csv")
#println(races[find(races[:Rnum].==89713),:])
#fred
	println("Calculating ", nrow(races))
	indexes = find(races[:PP] .== "F")
	deleterows!(races, indexes)

	indexes = find(races[:Date] .> 20150630)
	deleterows!(races, indexes)
	CSV.write(dir*"racesPP_20150630.csv", races)
	races = CSV.read(dir*"racesPP_20150630.csv")
#	adjust_VC!(races)

#	max = maximum(races[:Rnum])
#	println("the max is $max")
#	lines = CSV.read(dir*"lines.csv"; types=Dict("Head"=>String))
#	indexes = find(lines[:Rnum] .> max)
#	deleterows!(lines, indexes)
#	CSV.write(dir*"linesQUA_20150630.csv", lines)
	lines = CSV.read(dir*"linesPP_20150630.csv"; types=Dict("Head"=>String))

#    lines = CSV.read(dir*"lines.csv"; types=Dict("Head"=>String))
    println("Joining racelines")
    racelines = join(races, lines, on=[:Rnum])
	indexes = index_races(racelines)
    facts = load_fact_fns()
    ts =  CSV.read(ENV["DATADIR"]*"tracks.csv"; delim=':')
    racedates = CSV.read(ENV["DATADIR"]*"races.csv")
    dates = unique(racedates[:date])
    sort!(dates)

    for date in dates
        if date < sd continue
        elseif date > ed break end
        build_form(date, facts, ts, racelines, indexes)
    end
end

#################################################################
# Forms program

#SD = parse(Int64, ARGS[1])
#ED = parse(Int64, ARGS[2])
SD = 20150601
ED = 20150601
#ED = 20180701

process_races(SD, ED)
