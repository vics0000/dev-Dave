
@everywhere ENV["DATADIR"] = "/home/dave/Races/data/"
#@everywhere ENV["ALTDIR"] = "/home/dave/Races/amtote/altsource/"
@everywhere ENV["AMTDIR"] = "/home/dave/Races/dev/amtote/"
@everywhere ENV["HORSEDIR"] = "/home/dave/Races/dev/horses/"

@everywhere include(ENV["AMTDIR"]*"amt_type.jl")
@everywhere include(ENV["AMTDIR"]*"amt_util.jl")
#include("/home/whatwins/www/horses/models.jl")
@everywhere include(ENV["AMTDIR"]*"amt_xml.jl")
@everywhere include(ENV["AMTDIR"]*"amt_log.jl")
@everywhere include(ENV["HORSEDIR"]*"utils.jl")
@everywhere include(ENV["HORSEDIR"]*"play_utils.jl")
@everywhere include(ENV["HORSEDIR"]*"play.jl")
#@everywhere include(ENV["HORSEDIR"]*"history/utility_fns.jl")

@everywhere function process_race_days(sd, ed)
	date = sd

	while date <= ed
		println("\nBuilding odds for $date")
        races = Library()
        races.betEqns["Win"] = betDefn()
        races.betEqns["Win"].gaits["All"] = gaitDefn()
        races.betEqns["Win"].gaits["All"].gait["A"] = eqnDefn()
        races.betEqns["Win"].gaits["All"].gait["A"].eqn = CSV.read(ENV["DATADIR"] * "eqns/prod_win.csv")
        races.betEqns["WinR"] = betDefn()
        races.betEqns["WinR"].gaits["All"] = gaitDefn()
        races.betEqns["WinR"].gaits["All"].gait["A"] = eqnDefn()
        races.betEqns["WinR"].gaits["All"].gait["A"].func = "production_model"
        races.betEqns["WinR"].gaits["All"].gait["A"].model = load(ENV["DATADIR"] * "eqns/new_model.jld","new_model")
        races.betEqns["WinR"].gaits["All"].gait["A"].Cutoff = 0.11665258145340733
        races.betEqns["ExR"] = betDefn()
        races.betEqns["ExR"].gaits["All"] = gaitDefn()
        races.betEqns["ExR"].gaits["All"].gait["A"] = eqnDefn()
        races.betEqns["ExR"].gaits["All"].gait["A"].func = "production_model"
        races.betEqns["ExR"].gaits["All"].gait["A"].model = load(ENV["DATADIR"] * "eqns/new_ex.jld","ex_model")
        races.betEqns["ExR"].gaits["All"].gait["A"].Cutoff = -.2931
        races.betEqns["TriR"] = betDefn()
        races.betEqns["TriR"].gaits["All"] = gaitDefn()
        races.betEqns["TriR"].gaits["All"].gait["A"] = eqnDefn()
        races.betEqns["TriR"].gaits["All"].gait["A"].func = "production_model"
        races.betEqns["TriR"].gaits["All"].gait["A"].model = load(ENV["DATADIR"] * "eqns/new_tri.jld","tri_model")
        races.betEqns["TriR"].gaits["All"].gait["A"].Cutoff = -0.474
        races.source = "log"
	    races.inExt = "_Lo1"
	    races.outExt = "_log"
	    dir = ENV["DATADIR"] * "dailies/$date/"
	    file = "Bets$(races.outExt)"
        if isfile(dir * file * ".csv") rm(dir * file * ".csv") end
        file = "Odds$(races.outExt)"
        if isfile(dir * file * ".csv") rm(dir * file * ".csv") end
        file = "Prices$(races.outExt)"
        if isfile(dir * file * ".csv") rm(dir * file * ".csv") end
	    file = "Preds$(races.outExt)"
        if isfile(dir * file * ".csv") rm(dir * file * ".csv") end
	    file = "Forms$(races.outExt)"
        if isfile(dir * file * ".csv") rm(dir * file * ".csv") end
		dt = DateTime("$date", "yyyymmdd")
		dt = Dates.format(dt, "dduyy")
		dir = ENV["DATADIR"]*"Amtote/US/"
	    file = "OddsFeed_$(dt).zip"
	    if isfile(dir * file)
	        if !isfile("OddsFeed_$(dt).log")&&(!isfile("OddsFeed_$(uppercase(dt)).log"))
	            run(`unzip "$dir$file"`)
	        end
	        file = "OddsFeed_$(dt).log"
	        if !isfile(file) file = "OddsFeed_$(uppercase(dt)).log" end
	        println("Processing $file\n")
	        amt_process_file!(file, races)
	        rm(file)
	    end
fred
	    races.firstPost = DateTime()
	    races.ticker = DateTime()
	    races.changes = 0
		dir = ENV["DATADIR"]*"Amtote/US2/"
	    file = "OddsFeed_US2_$(dt).zip"
	    if isfile(dir * file)
	        if !isfile("OddsFeed_US2_$(dt).log")&&(!isfile("OddsFeed_US2_$(uppercase(dt)).log"))
	            run(`unzip "$dir$file"`)
	        end
	        file = "OddsFeed_US2_$(dt).log"
	        if !isfile(file) file = "OddsFeed_US2_$(uppercase(dt)).log" end
	        println("\nProcessing $file\n")
	        amt_process_file!(file, races)
	        rm(file)
	    end
	    date = add_day!(date)
	end
end

##############################################################################

#SD = parse(Int64, ARGS[1])
#ED = parse(Int64, ARGS[2])
SD = 20150601
ED = 20150607

process_race_days(SD, ED)
