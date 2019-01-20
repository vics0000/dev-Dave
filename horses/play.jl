
#@everywhere include("../horses/utility_fns_df.jl")
#@everywhere include("../horses/df_processing.jl")
#@everywhere include("../horses/factor_fns_df.jl")
#@everywhere include("../horses/predict_utils_df.jl")
#@everywhere include("../horses/odds_merge.jl")
#@everywhere include("../horses/derivative_ftrs_fns.jl")
#@everywhere include("../horses/exotics.jl")
#@everywhere include("../horses/csv_loading.jl")

# Define a callback function to receive data from the logs.
@everywhere function cb_amt_obj!(data::AmtCallbackData, races::Library)
    objtype = typeof(data.obj)
    if (objtype != GpAvailablePrograms) && invalid_track(data.track, races)
        if data.date < (races.ticker + Dates.Second(5)) return end
        post_races!(data.date, races)
        flush(STDOUT)
        return
    end
    if get(processDict, objtype, 0)==0
        println("Found an unknown object $error $objtype $data")
    else
	    try processDict[objtype](data, races)
	    catch error
	        println(error)
			if isa(error, KeyError)
			    raceNumber = objtype==GpItpData? data.obj.itpData.race.raceNumber: data.obj.race
			    println("Found a KeyError $(data.track) $raceNumber")
			    add_single_race!(data.track, raceNumber, races)
			end
            processDict[objtype](data, races)
	    end
    end
    post_races!(data.date, races)
    flush(STDOUT)
end

##############################################################################
# Functions to process the various messages from Amtote

@everywhere function post_races!(date::DateTime, races::Library)
    races.ticker = date
    if early(date, races)
        return
    end
    if live(races) && races.firstRun
        races.firstRun = false
		item = write_socket(races.sock, "requestAvailablePrograms\n")
		amt_parse_live!(item, "", races)
    end
    if races.changes > 2 redo_cards!(races) end

    for k in keys(races.Cards)
        card = races.Cards[k].CardInfo
        raceNumber = card.currentRace
        mtp = card.minutesToPost
        if (card.off == 1)&&(card.posted == 0)
            post_wagers!(date, k, raceNumber, races)
            continue
        end
        if (raceNumber==0)||(mtp > 0)||(card.off != 0)||(card.posted != 0) continue end
        if isempty(races.Cards[k].Races[raceNumber].CycleData.pools) continue end
        rnum = races.Cards[k].Races[raceNumber].rnum
        posted = races.Cards[k].Races[raceNumber].posted
        if (rnum==0)||(posted==1)
            if (posted==1)
                println("Previously posted $k $raceNumber")
                store_odds!(date, k, raceNumber, races)
            else
                races.Cards[k].Races[raceNumber].wagers = init_bets()
                println("No Rnum found $k $raceNumber")
            end
            races.Cards[k].CardInfo.posted = 1
            continue
        end
        if card.ticker == 9999
            races.Cards[k].CardInfo.pt = races.ticker + Dates.Second(card.delay)
            races.Cards[k].CardInfo.ticker = card.delay
        end
        if live(races) date = ceil(DateTime(now()), Dates.Second) end
        if check_time!(date, k, raceNumber, races) post_wagers!(date, k, raceNumber, races) end
    end
end

@everywhere function process_string(record::AmtCallbackData, races::Library)
#    println(record)
end

@everywhere function process_changeData(record::AmtCallbackData, races::Library)
#    println(record)
end

@everywhere function process_pool(record::AmtCallbackData, races::Library)
#    println(record)
end

@everywhere function process_pool(record::AmtCallbackData, races::Library)
#    println(record)
end

@everywhere function process_entry(record::AmtCallbackData, races::Library)
#    println(record)
end

@everywhere function process_carryin!(record::AmtCallbackData, races::Library)
    println(record)
end

@everywhere function process_combine(record::AmtCallbackData, races::Library)
    numberOfRaces = record.obj.definition.numberOfRaces
    track = record.obj.detail.programName
    longName = races.Cards[track].CardInfo.programLongName
    println("process_combine for $numberOfRaces races at $track $longName")

    for ii in 1:numberOfRaces
        if get(record.obj.definition.raceDetailList, ii, 99)==99 continue end
        scratches = record.obj.definition.raceDetailList[ii].scratchedRunners
        if !isempty(scratches)

            for jj in 1:length(scratches)
                runner = "$(scratches[jj])"
                changes = false
                rnum = races.Cards[track].Races[ii].rnum
                found = scratch_horse!(races, track, ii, runner)
		      	if found changes = check_selections!(track, ii, races) end
			    if changes
				    if early(record.date, races)
				        races.changes += 1
				    else
				      	races.Cards[track].Races[ii].posted = 0
				        redo_race!(rnum, races, track, ii)
				    end
			    end
            end
        end
	end
end

@everywhere function process_prices!(record::AmtCallbackData, races::Library)
    track = record.obj.programName
    raceNumber = record.obj.race
    bets = races.Cards[track].Races[raceNumber].wagers
    scratches = record.obj.priceInfo.scratches
    if scratches != ""
        println("Checking $track Race $raceNumber for late scratches: $scratches")
        check_late_scratches!(scratches, bets)
    end
    println("Checking payouts for $track Race $raceNumber")
    check_prices!(record, bets, races)
end

@everywhere function process_scratches!(record::AmtCallbackData, races::Library)
    status = strip(record.obj.status)
    if status == "S"
	    track = record.obj.programName
	    raceNumber = record.obj.race
	    runner = record.obj.runner
	    changes = false
	    rnum = races.Cards[track].Races[raceNumber].rnum
    	found = scratch_horse!(races, track, raceNumber, runner)
      	if found changes = check_selections!(track, raceNumber, races) end
	    if changes
		    if early(record.date, races)
		        races.changes += 1
		    else
		        races.Cards[track].Races[raceNumber].posted = 0
		        redo_race!(rnum, races, track, raceNumber)
		    end
	    end
    else println(record.obj) end
end

@everywhere function process_stop!(record::AmtCallbackData, races::Library)
    track = record.obj.programName
    raceNumber = record.obj.race
    races.Cards[track].CardInfo.off = 1
    if races.Cards[track].Races[raceNumber].off == 0
	    card = races.Cards[track].CardInfo
	    remaining = match(r"-?[0-9]+", "$((card.pt - record.date) / 1000)").match
	    remaining = -parse(Int64, remaining)
	    println("$track $(card.programLongName) Race $(raceNumber) remains $(remaining)s")
    end
    races.Cards[track].Races[raceNumber].off = 1
    store_off_odds!(record.date, track, raceNumber, races)
end

@everywhere function process_start!(record::AmtCallbackData, races::Library)
    track = record.obj.programName
    raceNumber = record.obj.race
    races.Cards[track].CardInfo.currentRace = raceNumber
    races.Cards[track].CardInfo.off = 0
    races.Cards[track].CardInfo.posted = 0
    races.Cards[track].CardInfo.ticker = 9999
    if live(races)
        item = write_socket(races.sock, "requestItpData $track $raceNumber $(races.Cards[track].CardInfo.programDate)\n")
		amt_parse_live!(item, track, races)
    end
end

@everywhere function process_mtp!(record::AmtCallbackData, races::Library)
    track = record.obj.programName
    raceNumber = record.obj.race
    if races.Cards[track].CardInfo.currentRace != raceNumber
        races.Cards[track].CardInfo.currentRace = raceNumber
	    races.Cards[track].CardInfo.off = 0
	    races.Cards[track].CardInfo.posted = 0
	    races.Cards[track].CardInfo.ticker = 9999
	    if live(races)
	        item = write_socket(races.sock, "requestItpData $track $raceNumber $(races.Cards[track].CardInfo.programDate)\n")
			amt_parse_live!(item, track, races)
	    end
    end
    if (record.obj.mtp<3)&&(record.obj.mtp>=0)&&(races.Cards[track].CardInfo.minutesToPost!=record.obj.mtp)
        println("$track Race $raceNumber MTP: ", record.obj.mtp)
    end
    races.Cards[track].CardInfo.minutesToPost = record.obj.mtp
    if strip(record.obj.postTime) !=""
        postTime = split(strip(record.obj.postTime), [':'])
        first = parse(Int64, postTime[1])<10? parse(Int64, postTime[1]) + 12: parse(Int64, postTime[1])
        postTime = "$first:$(parse(Int64, postTime[2]))"
		postTime = DateTime("$(Date(record.date)) $postTime", "yyyy-mm-dd H:M")
		if (races.firstPost==DateTime())||(postTime<races.firstPost)
			races.firstPost = postTime
			println("First race: $track $raceNumber at $(record.obj.postTime)")
		end
	    races.Cards[track].Races[raceNumber].ItpData.race.postTime = record.obj.postTime
    end
end

@everywhere function process_race!(record::AmtCallbackData, races::Library)
    ItpData = record.obj.itpData
    raceDate = ItpData.race.raceDate
    track = record.track
    date = record.date
    programDate = races.Cards[track].CardInfo.programDate
    if raceDate != programDate return end
    raceNumber = ItpData.race.raceNumber
    rnum = races.Cards[track].Races[raceNumber].rnum
    add_nums!(ItpData)
    races.Cards[track].Races[raceNumber].ItpData = ItpData
    changes = check_selections!(track, raceNumber, races)
    if changes
	    if early(date, races)
	        races.changes += 1
	    else redo_race!(rnum, races, track, raceNumber) end
    end
end

@everywhere function process_willPay!(record::AmtCallbackData, races::Library)
    exotic = record.obj.betType
    if get(races.Cards[record.track].Races, record.obj.race+1, 0) == 0
        add_single_race!(record.track, record.obj.race+1, races)
    end
    races.Cards[record.track].Races[record.obj.race + 1].willPaysList[exotic] = record.obj
end

@everywhere function process_cycle!(record::AmtCallbackData, races::Library)
    pools = record.obj.pools
    if !check_money(pools) return end
    raceNumber = record.obj.race
    track = record.track
    if isempty(races.Cards[track].Races[raceNumber].CycleData.pools)
        races.Cards[track].Races[raceNumber].CycleData.pools = pools
    end
    race = races.Cards[track].Races[raceNumber]
    if !money_change(pools, race) return end
    oldCycleData = races.Cards[track].Races[raceNumber].CycleData
    races.Cards[track].Races[raceNumber].CycleData = record.obj
    races.Cards[track].Races[raceNumber].oldCycleData = oldCycleData
    mtp = races.Cards[track].CardInfo.minutesToPost
    if mtp <= 1 check_odds!(races, track, raceNumber) end
end

@everywhere function process_defn!(record::AmtCallbackData, races::Library)
    data = record.obj.raceDetails
    raceNumber = data.raceNumber
    race = races.Cards[record.track].Races[raceNumber]
    data.rnum = race.rnum
    data.posted = race.posted
    data.off = race.off
    data.form = race.form
    data.wagers = race.wagers
    data.odds = race.odds
    data.ItpData = race.ItpData
    data.oldCycleData = race.oldCycleData
    data.savedCycleData = race.savedCycleData
    data.CycleData = race.CycleData
    data.willPaysList = race.willPaysList
    races.Cards[record.track].Races[raceNumber] = data
end

@everywhere function process_programs!(record::AmtCallbackData, races::Library)
    races.ticker = record.date
    programList = record.obj.programList
    if length(programList)==0 return end
    date = programList[1].programDate
    if date==0 return end
	if isempty(races.Cards)
	    forms, delays = get_daily_input(date)
        if forms==false return end
		get_rebates!(races)
		races.forms = forms
        races.delays = delays
        add_eqns!(races)
	else
	    forms = races.forms
	    delays = races.delays
	end

    for program in programList
        if program.programName in races.delays[:TEC]
            track = races.delays[findfirst(races.delays[:TEC], program.programName), :Track]
            TEC = program.programName
            if get(races.Cards, TEC, 0) == 0
	            data = RaceData()
	            data.Track = track
	            data.CardInfo = program
	            data.CardInfo.delay = add_delay(TEC, delays)
	            println("Loading: $track $TEC ", program.programLongName)
	            races.Cards[TEC] = data
	            races.Cards[TEC].Races = add_races!(track, forms, TEC, races, true)
                if live(races) add_program_details!(TEC, races) end
            end
        else println("$(program.programName) ", program.programLongName) end
        flush(STDOUT)
    end
    println()
end

const processDict = Dict(UpdateRaceTimes=>process_mtp!,
		                GpCycleData=>process_cycle!,
		                WillPays=>process_willPay!,
		                GpItpData=>process_race!,
		                GpPoolHolder=>process_pool,
		                GpProgramCombine=>process_combine,
		                NotifyStartBetting=>process_start!,
		                NotifyStopBetting=>process_stop!,
		                GpPrices=>process_prices!,
		                NotifyRunnerStatus=>process_scratches!,
		                NotifyChangeData=>process_changeData,
		                NotifyEntryRunnerStatus=>process_entry,
		                GpRaceDefinition=>process_defn!,
		                NotifyPoolCarryIn=>process_carryin!,
		                Void=>process_string,
		                String=>process_string,
		                GpAvailablePrograms=>process_programs!)
