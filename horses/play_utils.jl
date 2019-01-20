# play_utils.jl

using DataArrays, DataFrames

#@everywhere function DB_connect()
#    MySQL.connect("whatwins.com", "whatwins_user", "db*pw4WW", db="whatwins_data")
#end

@everywhere function live(races::Library)
    live = races.source=="live"? true: false
end

@everywhere function early(date::DateTime, races::Library)
    early = false
    if isempty(races.Cards)||(races.firstPost==DateTime())||(date<races.firstPost - Minute(50))
        early = true
    end
    early
end

@everywhere function valid_race(track::String, raceNumber::Int64, races::Library)
    valid = true
    race = races.Cards[track].Races[raceNumber]
    horses = collect(1:size(race.form)[1])
    formHeads = race.form[horses, :Head]
    horses = length(formHeads)
	uniqueHorses = length(unique(formHeads))

    rnum = race.rnum
    entries = length(race.ItpData.starters)

    for horse in race.ItpData.starters
        heads = find(formHeads .== uppercase(horse.programNumber))
        pn = horse.programNumber
        pp = horse.postPosition
        if length(heads) > 1
			valid = false
			println("$rnum $track $raceNumber - not a valid race; $(length(heads)) Head $pn Post $pp")
        end
    end
    if horses > uniqueHorses
		valid = false
		println("$rnum $track $raceNumber - not a valid race; mismatch; form:$horses unique:$uniqueHorses")
    end
    if horses <= 3
		valid = false
		println("$rnum $track $raceNumber - not a valid race; form:$horses horses")
    end
    valid
end

@everywhere function post_wagers!(dateTime::DateTime, track::String, raceNumber::Int64, races::Library)
    programName = strip(races.Cards[track].CardInfo.programLongName)
    races.Cards[track].CardInfo.posted = 1
    store_odds!(dateTime, track, raceNumber, races)
    races.Cards[track].Races[raceNumber].posted = 1
    if races.Cards[track].CardInfo.off == 1
        println("Missed $programName Race $raceNumber")
        return
    end
	println("\nPosting $programName Race $raceNumber")
    if valid_race(track, raceNumber, races)
        if live(races) store_bets!(dateTime, track, raceNumber, races) end
    end
end

@everywhere function check_time!(dateTime::DateTime, track::String, raceNumber::Int64, races::Library)
    card = races.Cards[track]
    programName = strip(card.CardInfo.programLongName)
	if dateTime < card.CardInfo.pt
	    remaining = match(r"-?[0-9]+", "$((card.CardInfo.pt - dateTime) / 1000)").match
	    remaining = parse(Int64, remaining)
	    if remaining >= (card.CardInfo.ticker - 10) return false end
        println("Posting $programName $raceNumber in $(remaining)s")
        races.Cards[track].CardInfo.ticker = remaining
        post = false
    else post = true end
end

@everywhere function add_eqns!(races::Library)
    eqns = get_best_suffix(races.inExt)
    races.best = eqns

    for row in 1:nrow(eqns)
        if get(races.betEqns, eqns[row,1], 0) == 0 races.betEqns[eqns[row,1]] = betDefn() end
        if get(races.betEqns[eqns[row,1]].gaits, eqns[row,8], 0) == 0
            races.betEqns[eqns[row,1]].gaits[eqns[row,8]] = gaitDefn()
        end
        if get(races.betEqns[eqns[row,1]].gaits[eqns[row,8]].gait, eqns[row,10], 0) == 0
            races.betEqns[eqns[row,1]].gaits[eqns[row,8]].gait[eqns[row,10]] = eqnDefn()
        end
        races.betEqns[eqns[row,1]].gaits[eqns[row,8]].gait[eqns[row,10]].func = eqns[row,2]
        races.betEqns[eqns[row,1]].gaits[eqns[row,8]].gait[eqns[row,10]].tt = eqns[row,7]
        races.betEqns[eqns[row,1]].gaits[eqns[row,8]].gait[eqns[row,10]].kind = eqns[row,9]
        races.betEqns[eqns[row,1]].gaits[eqns[row,8]].gait[eqns[row,10]].Intercept = eqns[row,4]
        races.betEqns[eqns[row,1]].gaits[eqns[row,8]].gait[eqns[row,10]].Coefficient = eqns[row,5]
        races.betEqns[eqns[row,1]].gaits[eqns[row,8]].gait[eqns[row,10]].Cutoff = eqns[row,11]
    end
end

@everywhere function add_delay(TEC::String, delays::DataFrame)
    delays[findfirst(delays[:TEC], TEC), :Delay]
end

@everywhere function add_single_race!(TEC::String, raceNumber::Int64, races::Library)
    rDef = RaceDefinition()
    forms = races.forms
    Track = races.delays[findfirst(races.delays[:TEC], TEC), :Track]
    track_dps = trim_track(forms, Track)
    indexes = find(forms[:Race] .== raceNumber)
    index = fast_intersect(indexes, track_dps)
    if isempty(index)
        rDef.wagers = init_bets()
        println("\nCouldn't find $Track $TEC Race $raceNumber in forms")
    else
        rDef.wagers = init_bets()
		rDef.rnum = forms[index[1], :Rnum]
		rDef.form = forms[find(forms[:Rnum] .== rDef.rnum), :]
        println("Adding single $TEC Race $raceNumber")
    end
    rDef.raceNumber = raceNumber
    races.Cards[TEC].Races[raceNumber] = rDef
end

@everywhere function add_program_details!(TEC::String, races::Library)
    program = races.Cards[TEC].CardInfo
    programDate = program.programDate
    sock = races.sock
    item = write_socket(sock, "requestProgramCombine $TEC\n")
	amt_parse_live!(item, TEC, races)

    for ii in 1:program.highestRace
        item = write_socket(sock, "requestItpData $TEC $ii $programDate\n")
		amt_parse_live!(item, TEC, races)
	end
end

@everywhere function add_races!(track::String, forms::DataFrame, TEC::String, races::Library, verbose=false)
    track_dps = trim_track(forms, track)
    racesDetails = Dict{Int64, RaceDefinition}()
    program = races.Cards[TEC].CardInfo

    for ii in 1:program.highestRace
        rDef = (get(races.Cards[TEC].Races, ii, 0)==0)? RaceDefinition(): races.Cards[TEC].Races[ii]
        indexes = find(forms[:Race] .== ii)
        index = fast_intersect(indexes, track_dps)
        if isempty(index) println("Couldn't find $track Race $ii in forms")
        else
			rDef.rnum = forms[index[1], :Rnum]
			rDef.form = forms[find(forms[:Rnum] .== rDef.rnum), :]
			rDef.wagers = init_bets()
            if verbose println("Adding $track Race $ii") end
        end
		rDef.raceNumber = ii
		racesDetails[ii] = rDef
	end
	racesDetails
end

@everywhere function check_money(pools::Array{GpPool})

    for pool in pools
        poolTotal = 0
        if pool.poolTotal != "RF" poolTotal = parse(Int64, pool.poolTotal) end
        if poolTotal > 0 return true end
    end
    false
end

@everywhere function money_change(pools::Array{GpPool}, race::RaceDefinition)
    oldPools = race.CycleData.pools
    index = 1

    for pool in pools
        poolTotal = (pool.poolTotal!="RF")?parse(Int64, pool.poolTotal):0
        oldTotal = 0
        if length(oldPools)>=index
            oldTotal = (oldPools[index].poolTotal!="RF")?parse(Int64, oldPools[index].poolTotal):0
        end
        if poolTotal > oldTotal return true end
        index += 1
    end
    false
end

@everywhere function get_rebates!(races::Library)
    con = DB_connect()
    command = "SELECT * FROM Rebates"
    races.Rebates = MySQL.query(con, command, DataFrame)
    MySQL.disconnect(con)
end

@everywhere function Hnum(entry::Selection)
    horse = replace(entry.name, "`", "")
    horse = uppercase(replace(horse, r"[\-\s+]", " "))
	horse = strip(match(r"[A-Z| ]+", horse).match)
    sex = uppercase(entry.sex)
    age = entry.yearOfBirth
    con = DB_connect()
    command = "SELECT Hnum FROM Horses WHERE Name='$horse'"
    hnum = MySQL.query(con, command, DataFrame)
	if isempty(hnum[:Hnum])
	 	command = "INSERT INTO Horses (Name) VALUES ('$horse')";
	    MySQL.execute!(con, command)
	end
    command = "SELECT Hnum FROM Horses WHERE Name='$horse'"
    hnum = MySQL.query(con, command, DataFrame)
	command = "UPDATE Horses SET Name='$horse',Sex='$sex',Age=$age WHERE Hnum=$(hnum[1, :Hnum])"
	MySQL.execute!(con, command)
    MySQL.disconnect(con)
    hnum[1, :Hnum]
end

@everywhere function DnumTnum(name::String, role::String="Driver")
    name = replace(name, r"[\.\`\(\)\[\]]", "")
    name = strip(uppercase(replace(name, r"[\-\s+]", " ")))
    con = DB_connect()
    DBname = name
    table = role=="Driver"? "Drivers": "Trainers"
    col = role=="Driver"? "DriverID": "TrainerID"
    command = "SELECT $col FROM $table WHERE Name='$DBname'"
    num = MySQL.query(con, command, DataFrame)
	if isempty(num[Symbol(col)])
	    names = split(name)
	    positions = length(names)
	    last = ""
	    first = replace(names[1], r"[\.]", "")
        for ii in 2:positions last *= names[ii] * " " end
	    for ii in 1:length(first)
	        DBname = strip(first[1:end-ii] * " " * last)
	        command = "SELECT $col, Name FROM $table WHERE Name='$DBname'"
	        num = MySQL.query(con, command, DataFrame)
            if !isempty(num[Symbol(col)]) break end
            pattern = "^$(first[1:end-ii+1])[A-Z]* $(strip(last))\$"
	        command = "SELECT $col, Name FROM $table WHERE Name REGEXP '$pattern'"
	        num = MySQL.query(con, command, DataFrame)
            if !isempty(num[Symbol(col)]) break end
        end
        if isempty(num[Symbol(col)])&&(length(names)>2)
            last = ""
            for ii in 3:positions last *= names[ii] * " " end
	        DBname = strip(first * " " * last)
	        command = "SELECT $col, Name FROM $table WHERE Name='$DBname'"
	        num = MySQL.query(con, command, DataFrame)
        end
	    if !isempty(num[Symbol(col)])&&(length(name)>length(num[:Name]))
            command = "UPDATE $table SET Name='$name' WHERE $col=$(num[1, Symbol(col)])"
	        MySQL.execute!(con, command)
        end
    end
	if isempty(num[Symbol(col)])
	    println("New $role $name")
	 	command = "INSERT INTO $table (Name) VALUES ('$name')";
	    MySQL.execute!(con, command)
	    command = "SELECT $col FROM $table WHERE Name='$name'"
	    num = MySQL.query(con, command, DataFrame)
	end
    MySQL.disconnect(con)
    num[1, Symbol(col)]
end

@everywhere function add_nums!(data::ItpData)

    for starter in data.starters
        starter.hnum = Hnum(starter)
        starter.tnum = DnumTnum(starter.trainerName, "Trainer")
        starter.dnum = DnumTnum(starter.jockeyName)
    end
end

@everywhere function change_DB(rnum::Int64, horse::Selection)
    con = DB_connect()
    hnum = horse.hnum
    dnum = horse.dnum
    tnum = horse.tnum
    head = uppercase(horse.programNumber)
    post = horse.postPosition
    mline = !isempty(strip(horse.morningLine))? horse.morningLine: "4"
    mline = split(mline, ['/','-'])
    mline = length(mline)==2? parse(Int64, mline[1])/parse(Int64, mline[2]): parse(Int64, mline[1])
    command = "UPDATE WWLines SET Post=$post,Head='$head',Tnum=$tnum,Dnum=$dnum,Mline=$mline WHERE Hnum=$hnum AND Rnum=$rnum"
    MySQL.execute!(con, command)
    MySQL.disconnect(con)
end

@everywhere function change_form_DB(rnum::Int64, hnum::Int64)
    con = DB_connect()
    command = "UPDATE WWLines SET Post=0 WHERE Hnum=$hnum AND Rnum=$rnum"
    MySQL.execute!(con, command)
    MySQL.disconnect(con)
end

@everywhere function check_details(rnum::Int64, horse::Selection, form::DataFrame, index::Int64)
    changes = false
    change = "$(form[index, :Track]) $(form[index, :Race]) $rnum $(horse.programNumber) $(horse.name) "
    if horse.tnum != form[index, :Tnum]
        changes = true
        change *= "Trainer $(horse.trainerName) $(horse.tnum) $(form[index, :Tnum]) "
    end
    if horse.dnum != form[index, :Dnum]
        changes = true
        change *= "Driver $(horse.jockeyName) $(horse.dnum) $(form[index, :Dnum]) "
    end
    if uppercase(horse.programNumber) != "$(form[index, :Head])"
        changes = true
        change *= "Head $(horse.programNumber) "
    end
    horse.postPosition = horse.postPosition!=99? horse.postPosition: 0
    if horse.postPosition < form[index, :Post]
        changes = true
        change *= "Post $(horse.postPosition) "
    end
    mline = !isempty(strip(horse.morningLine))? horse.morningLine: "4"
    mline = split(mline, ['/','-'])
    mline = length(mline)==2? parse(Int64, mline[1])/parse(Int64, mline[2]): parse(Int64, mline[1])
    if mline != form[index, :MLine]
        changes = true
        change *= "ML $(horse.morningLine) MLform $(form[index, :MLine])"
    end
    if changes
        println(change)
        change_DB(rnum, horse)
    end
    changes
end

@everywhere function get_latest_entries(rnum::Int64)
    con = DB_connect()
    command = "SELECT * FROM Races WHERE Rnum=$rnum"
    race = MySQL.query(con, command, DataFrame)
	race[:Date] = map(x->parse(Int64, replace("$x", "-", "")), race[:Date])
	race[:TS] = add_tracksize!(race)
    command = "SELECT * FROM WWLines where Rnum=$rnum AND Post>0"
    lines = MySQL.query(con, command, DataFrame)
    MySQL.disconnect(con)
	unique!(lines, [:Post])
	entries = join(race, lines, on = [:Rnum])
	sort!(entries, [:Post])
	delete!(entries, [:Perf,:Temp,:Type,:WEP,:EEP,:TEP,:WPool,:EPool,:TPool,:Processed,:LorCorLC,:Check])
	entries
end

@everywhere function redo_check_factor!(data::Dict, form::DataFrame, ff, horses::DataFrame, lines::DataFrame)
	exceptions = ["MLinR"]
	if ((Symbol(ff[1]) in(names(form)))&!(ff[1] in(exceptions))&(nrow(form)==nrow(horses))) return end
	form[Symbol(ff[1])] = round.(ff[2](ff[1], data, horses, lines), 4)
end

@everywhere function build_race_form!(data::Dict, horses::DataFrame, factors::DataFrame)
	form = copy(horses[ [:Rnum, :Hnum, :Post]])
	form[:Targ] = 0.0
	lines = get_lines(data["cards"], horses)
    markup_lines!(lines, horses,  Symbol("Tnum"), data["tracks"])

	for ff in(factors[:ff])
	    redo_check_factor!(data, form, ff, horses, lines)
	end
    form[:Entry] = missing
	form = format_forms(form)
    delete!(horses, [:Stat])
	form = join(horses, form, on = [:Rnum, :Hnum, :Post])
	indexes = collect(1:nrow(form))
    set_complex_factors!(form, indexes)
    delete!(form, [:Targ])
    trim_forms!(form)
	date = horses[1,:Date]
    merge_formated_form(form, date)
    form
end

@everywhere function save_odds!(odds_df::DataFrame, e::OddsDB)
    record = [e.Rnum, e.Date, e.Source, e.Kind, e.Timestamp, e.Exotic, e.First, e.Second, e.Value]
    push!(odds_df, record)
end

@everywhere function save_prices!(prices_df::DataFrame, e::priceDB)
    record = [e.Rnum, e.Date, e.Source, e.poolID, e.baseValue, e.name, e.results,
              e.reasonX, e.reasonY, e.reasonZ, e.paid]
    push!(prices_df, record)
end

@everywhere function price_entry!(rnum::Int64, date::Int64, source::String, prices::DataFrame, data::PriceRecord)
    e = priceDB()
    e.Rnum = rnum
    e.Date = date
    e.Source = source
    e.poolID = data.poolID
    e.baseValue = data.baseValue
    e.name = data.name
    e.results = data.results
    e.reasonX = data.reasonX
    e.reasonY = data.reasonY
    e.reasonZ = data.reasonZ
    e.paid = data.paid
    save_prices!(prices, e)
end

@everywhere function pool_entry!(rnum::Int64, date::Int64, source::String, timestamp::String, odds::DataFrame, data::GpPool)
    e = OddsDB()
    e.Rnum = rnum
    e.Date = date
    e.Source = source
    e.Kind = "Money"
    e.Timestamp = timestamp
    e.Exotic = data.betType
    e.Value = (data.poolTotal!="RF")?parse(Float64, data.poolTotal):0
    save_odds!(odds, e)

    for ii in 1:data.nrRows
        money = split(data.money[ii])
        for jj in 1:data.nrValuesPerRow
            if (money[jj]!="SC")&&(money[jj]!="-")
                e.First = jj
                if data.nrRows > 1
                    e.First = ii
                    e.Second = jj
                end
                e.Value = (money[jj]!="NL")?parse(Float64, money[jj]):0
                save_odds!(odds, e)
            end
        end
    end
end

@everywhere function prob_entry!(rnum::Int64, date::Int64, source::String, timestamp::String, odds::DataFrame, data::GpProbs)
    e = OddsDB()
    e.Rnum = rnum
    e.Date = date
    e.Source = source
    e.Kind = "Pays"
    e.Timestamp = timestamp
    e.Exotic = data.betType
    betType = data.betType

    for ii in 1:data.nrRows
        probs = split(data.probs[ii])
        for jj in 1:data.nrValuesPerRow
            if (probs[jj]!="SC")&&(probs[jj]!="-")
                e.First = jj
                if ((betType=="PL")||(betType=="SH"))&&(ii==2) e.Exotic = betType * "$ii"
                elseif (data.nrRows > 1)&&(betType!="PL")&&(betType!="SH")
                    e.First = ii
                    e.Second = jj
                end
                e.Value = (probs[jj]!="NL")?parse(Float64, probs[jj]):0
                save_odds!(odds, e)
            end
        end
    end
end

@everywhere function odds_entry!(rnum::Int64, date::Int64, source::String, timestamp::String, odds::DataFrame, data::GpOdds)
    e = OddsDB()
    e.Rnum = rnum
    e.Date = date
    e.Source = source
    e.Kind = "Odds"
    e.Timestamp = timestamp
    e.Exotic = data.betType

    for ii in 1:data.nrRows
        odds_a = split(data.odds[ii])
        for jj in 1:data.nrValuesPerRow
            if (odds_a[jj]!="SC")&&(odds_a[jj]!="-")
                e.First = jj
                if data.nrRows > 1
                    e.First = ii
                    e.Second = jj
                end
                line = (odds_a[jj]!="NL")?odds_a[jj]:"0"
                line = split(line, ['/','-'])
                line = length(line)==2?round.(parse(Float64,line[1])/parse(Float64,line[2]),3):parse(Float64,line[1])
                e.Value = line
                save_odds!(odds, e)
            end
        end
    end
end

@everywhere function willpay_entry!(rnum::Int64, date::Int64, source::String, odds::DataFrame, data::WillPays)
    e = OddsDB()
    e.Rnum = rnum
    e.Date = date
    e.Source = source
    e.Kind = "WPays"
    e.Exotic = data.betType

    for ii in 1:length(data.willPayList)
        entry = data.willPayList[ii]
        result = split(entry.result, ['/','-'])
        result = result[end]
        e.First = parse(Int64, result)
        e.Timestamp = entry.reason
        v = strip(entry.theValue)
        e.Value = (v!="SC")&&(v!="NM")? parse(Float64, v): 0
        save_odds!(odds, e)
    end
end

@everywhere function store_odds!(dateTime::DateTime, track::String, raceNumber::Int64, races::Library)
	println("track $track raceNumber $raceNumber")
    if nrow(races.Cards[track].Races[raceNumber].odds)>0 return end
    odds_df = init_odds()
    race = races.Cards[track].Races[raceNumber]
    savedPools = race.savedCycleData.pools
    savedOdds = race.savedCycleData.odds
    savedProbs = race.savedCycleData.probs
    pools = race.CycleData.pools
    odds = race.CycleData.odds
    probs = race.CycleData.probs
    source = live(races)? "SIM": "Log"
    programDate = races.Cards[track].CardInfo.programDate

    for willpay in race.willPaysList
        willpay_entry!(race.rnum, programDate, source, odds_df, willpay[2])
    end

    for ii in 1:length(pools)
        pool_entry!(race.rnum, programDate, source, "last", odds_df, pools[ii])
        betType = pools[ii].betType
        for jj in 1:length(probs)
            if betType == probs[jj].betType
                if (length(savedPools)>=ii) pool_entry!(race.rnum, programDate, source, "saved", odds_df, savedPools[ii]) end
                prob_entry!(race.rnum, programDate, source, "last", odds_df, probs[jj])
                if (length(savedProbs)>=jj) prob_entry!(race.rnum, programDate, source, "saved", odds_df, savedProbs[jj]) end
                for kk in 1:length(odds)
                    if betType == odds[kk].betType
                        odds_entry!(race.rnum, programDate, source, "last", odds_df, odds[kk])
                        if (length(savedOdds)>=kk) odds_entry!(race.rnum, programDate, source, "saved", odds_df, savedOdds[kk]) end
                    end
                end
            end
        end
    end
    append_file(odds_df, programDate, "Odds", races.outExt)
    races.Cards[track].Races[raceNumber].odds = odds_df
    races.Cards[track].Races[raceNumber].willPaysList = Dict{String,WillPays}()
end

@everywhere function store_off_odds!(dateTime::DateTime, track::String, raceNumber::Int64, races::Library)
    odds_df = init_odds()
    race = races.Cards[track].Races[raceNumber]
    savedPools = race.savedCycleData.pools
    savedOdds = race.savedCycleData.odds
    savedProbs = race.savedCycleData.probs
    pools = race.CycleData.pools
    odds = race.CycleData.odds
    probs = race.CycleData.probs
    source = live(races)? "SIM": "Log"
    programDate = races.Cards[track].CardInfo.programDate

    for ii in 1:length(pools)
        pool_entry!(race.rnum, programDate, source, "last_o", odds_df, pools[ii])
        betType = pools[ii].betType
        for jj in 1:length(probs)
            if betType == probs[jj].betType
                if (length(savedPools)>=ii) pool_entry!(race.rnum, programDate, source, "saved_o", odds_df, savedPools[ii]) end
                prob_entry!(race.rnum, programDate, source, "last_o", odds_df, probs[jj])
                if (length(savedProbs)>=jj) prob_entry!(race.rnum, programDate, source, "saved_o", odds_df, savedProbs[jj]) end
                for kk in 1:length(odds)
                    if betType == odds[kk].betType
                        odds_entry!(race.rnum, programDate, source, "last_o", odds_df, odds[kk])
                        if (length(savedOdds)>=kk) odds_entry!(race.rnum, programDate, source, "saved_o", odds_df, savedOdds[kk]) end
                    end
                end
            end
        end
    end
    append_file(odds_df, programDate, "Odds", races.outExt)
    races.Cards[track].Races[raceNumber].odds = odds_df
end

@everywhere function store_final_odds!(dateTime::DateTime, track::String, raceNumber::Int64, races::Library)
    odds_df = init_odds()
    race = races.Cards[track].Races[raceNumber]
    odds = race.CycleData.odds
    pools = race.CycleData.pools
    probs = race.CycleData.probs
    if length(pools)>0
        if pools[1].cycleType != "F" return end
    end
    source = live(races)? "SIM": "Log"
    programDate = races.Cards[track].CardInfo.programDate

    for ii in 1:length(pools)
        pool_entry!(race.rnum, programDate, source, "final", odds_df, pools[ii])
        betType = pools[ii].betType
        for jj in 1:length(probs)
            if betType == probs[jj].betType
                prob_entry!(race.rnum, programDate, source, "final", odds_df, probs[jj])
                for kk in 1:length(odds)
                    if betType == odds[kk].betType
                        odds_entry!(race.rnum, programDate, source, "final", odds_df, odds[kk])
                    end
                end
            end
        end
    end
    append_file(odds_df, programDate, "Odds", races.outExt)
    races.Cards[track].Races[raceNumber].CycleData = GpCycleData()
end

@everywhere function augment_forms!(track::String, raceNumber::Int64, races::Library)
    race = races.Cards[track].Races[raceNumber]
    forms_df = race.form
    odds_df = race.odds
    dp_dict = dps_by_rnum(forms_df)
    add_track_size!(forms_df)
    add_ts_facts!(forms_df)
    add_baby_facts!(forms_df)
    add_angle_facts!(forms_df)
    add_pool_size!(forms_df, dp_dict, odds_df)
    rnums = unique(forms_df[:Rnum])
    entry_index_map = create_entry_index_map(forms_df, dp_dict, rnums)
    init_odds_facts!(forms_df)
    odds_dp_dict = dps_by_rnum(odds_df)
    add_odds_facts_single_race!(forms_df, dp_dict, entry_index_map, odds_df, odds_dp_dict, odds_df[:Rnum][1])
    clean_odds_facts!(forms_df)
    initialize_odds_fields(forms_df)

    for ii in [1:size(odds_df)[1];]
        merge_single_line!(forms_df, dp_dict, entry_index_map, odds_df, ii)
    end
    mline_prob_all!(forms_df,dp_dict)
    forms_df[:__ps_prior_prob] = 1.0 / length(forms_df[:__po_Odds_Win])
    races.Cards[track].Races[raceNumber].form = forms_df
end

@everywhere function predict_race_historical!(track::String, raceNumber::Int64, races::Library)
	target_fns = ["Win"=>1, "Ex"=>2, "Tri"=>3]
    preds = initialize_card()
    race = races.Cards[track].Races[raceNumber]
    forms = race.form
    dps = collect(1:size(race.form)[1])
    tt = forms[dps, :TS][1]
    kind = forms[dps, :Baby][1]==1? "B": forms[dps, :Gait][1]

	for bet in target_fns
	    prediction = init_wagers(forms, dps, bet[1], bet[2])
	    func = races.betEqns[bet[1]].gaits[tt].gait[kind].func
	    ptt = races.betEqns[bet[1]].gaits[tt].gait[kind].tt
	    pkind = races.betEqns[bet[1]].gaits[tt].gait[kind].kind
        extra = [tt, ptt, kind, pkind, func]
        linear = ismatch(r"linear", func)
        normalize = ismatch(r"normalized", func)
        fit_name = ismatch(r"orthogonal", func)? "orthogonal": "forward_stepwise"
        if linear
            prediction[:Prob] = linear_xterms!(races, forms, dps, bet[1], fit_name, normalize, extra)
        else
            prediction[:Prob] = amalgamated_xterms!(races, forms, dps, bet[1], fit_name, normalize, extra)
        end
	    func = races.betEqns[bet[1]*"R"].gaits[tt].gait[kind].func
	    ptt = races.betEqns[bet[1]*"R"].gaits[tt].gait[kind].tt
	    pkind = races.betEqns[bet[1]*"R"].gaits[tt].gait[kind].kind
        extra = [tt, ptt, kind, pkind, func]
        linear = ismatch(r"linear", func)
        normalize = ismatch(r"normalized", func)
        fit_name = ismatch(r"orthogonal", func)? "orthogonal": "forward_stepwise"
        if linear
            prediction[:ROI] = linear_xterms!(races, forms, dps, bet[1]*"R", fit_name, normalize, extra)
        else
            prediction[:ROI] = amalgamated_xterms!(races, forms, dps, bet[1]*"R", fit_name, normalize, extra)
        end
        prediction[:ROIold] = prediction[:ROI]
        prediction[:Probold] = prediction[:Prob]
        add_ProbO!(prediction)
        prediction[:ProbOo] = prediction[:ProbO]
	    add_ROIO!(prediction)
        prediction[:ROIOo] = prediction[:ROIO]
	    preds = [preds; prediction]
	end
	preds
end

@everywhere function combine_entries(preds::DataFrame, track::String, raceNumber::Int64, races::Library)
    entry = false
    wins = find(preds[:Exotic] .== "win")
    temp = preds

    for win in wins
        v = tryparse(Int64, preds[win, :First])
        if (isnull(v))
			entry = true
			break
        end
    end
    if entry
        temp = initialize_card()
        sort!(preds, cols = [order(:Exotic, lt = wetlt), :First, :Second, :Third])

        for row in 1:nrow(preds)
		    v1 = match(r"[0-9]+", preds[row, :First]).match
		    v2 = match(r"[0-9]+", preds[row, :Second]).match
		    v3 = match(r"[0-9]+", preds[row, :Third]).match
		    index1 = find(temp[:First] .== v1)
		    index2 = fast_intersect(index1, find(temp[:Second] .== v2))
		    index3 = fast_intersect(index2, find(temp[:Third] .== v3))
		    if isempty(index3)
		        tempRow = preds[row, :]
		        tempRow[1, :First] = v1
		        tempRow[1, :Second] = v2
		        tempRow[1, :Third] = v3
		        append!(temp, tempRow)
		    else
				temp[index3, :Prob] += preds[row, :Prob]
				temp[index3, :ROI] += preds[row, :ROI]
            end
        end
    end
    winPreds = preds[find(preds[:Exotic] .== "win"), :]
    race = races.Cards[track].Races[raceNumber]
    winPreds[:Name] = "Unknown"

    for horse in race.ItpData.starters
        winPreds[find(winPreds[:First] .== strip(uppercase(horse.programNumber))), :Name] = horse.name
    end
	winPreds[:Prob] = round.(winPreds[:Prob], 3)
    winPreds[:ROI] = round.(winPreds[:ROI], 3)
	sort!(winPreds, order(:Prob, rev = true))
    println(winPreds[[:First, :Name, :ProbOo, :ROIO, :ROIOo, :Prob, :ROI]])
	sort!(temp, (order(:Exotic, lt = wetlt), order(:ROI, rev = true)))
    preds = temp
end

@everywhere function save_bets!(bets_df::DataFrame, e::wagerDB)
    record = [e.Rnum, e.Date, e.ModelVal, e.ModelTag, e.ModelHash, e.Track, e.Race, e.Source, e.Exotic,
              e.First, e.Second, e.Third, e.Fourth, e.Fifth, e.Tickets, e.Wager, e.Balance, e.aWager]
    push!(bets_df, record)
end

@everywhere function wager_entry!(track::String, TEC::String, raceNumber::Int64, source::String, pred::DataFrame,
                                  wager::Float64, races::Library)
    e = wagerDB()
    e.Rnum = races.Cards[TEC].Races[raceNumber].rnum
    e.Date = races.Cards[TEC].CardInfo.programDate
    e.ModelVal = round.(pred[1, :ROI], 6)
    e.Track = track
    e.Race = raceNumber
    e.Source = source
    e.Exotic = pred[1, :Exotic]
    e.First = parse(Int64, pred[1, :First])
    e.Second = parse(Int64, pred[1, :Second])
    e.Third = parse(Int64, pred[1, :Third])
    e.Tickets = 1
    e.Wager = wager
    e
end

@everywhere function get_bet_mins(bet::String, betTypeInformation::Array{BetTypeInformation})
    minBaseAmount = 2.0
    multipleAmount = 1.0

	for ii in 1:length(betTypeInformation)
	    if betTypeInformation[ii].betTypeName==bet
			minBaseAmount = betTypeInformation[ii].minBaseAmount
			multipleAmount = betTypeInformation[ii].multipleAmount
			break
	    end
    end
    minBaseAmount, multipleAmount
end

@everywhere function get_bet_pays(bet::String, first::Int64, second::Int64, odds::DataFrame)
    indexes = find(odds[:Kind] .== "Money")
    indexes = fast_intersect(find(odds[:Timestamp] .== "last"), indexes)
    indexes = fast_intersect(find(odds[:Exotic] .== bet), indexes)
    pindex = fast_intersect(find(odds[:First] .== 0), indexes)
    hindexes = fast_intersect(find(odds[:First] .== first), indexes)
    bindex = fast_intersect(find(odds[:Second] .== second), hindexes)
    pool = odds[pindex, :Value][1]
	try money = odds[bindex, :Value][1]
	catch error return 0 end
    money = odds[bindex, :Value][1]
    pays = round.(pool / money, 2)
end

@everywhere function valid_bet(pred::DataFrame, TEC::String, raceNumber::Int64, races::Library)
    Exotics = Dict("win"=>"WN", "ex"=>"EX", "tri"=>"TR")
    Stats = Dict("win"=>"WinR", "ex"=>"ExR", "tri"=>"TriR")
    stat = Stats[pred[1, :Exotic]]
    race = races.Cards[TEC].Races[raceNumber]
    forms = race.form
    dps = collect(1:size(race.form)[1])
    tt = forms[dps, :TS][1]
    kind = forms[dps, :Baby][1]==1? "B": forms[dps, :Gait][1]
    if pred[1, :Exotic]=="win"
        cutoff = races.betEqns["WinR"].gaits["All"].gait["A"].Cutoff
    elseif pred[1, :Exotic]=="ex"
        cutoff = races.betEqns["ExR"].gaits["All"].gait["A"].Cutoff
    elseif pred[1, :Exotic]=="tri"
        cutoff = races.betEqns["TriR"].gaits["All"].gait["A"].Cutoff
    else
        cutoff = races.betEqns[stat].gaits[tt].gait[kind].Cutoff
    end
    betTypeInformation = race.betTypeInformation
    wager = 0
    if pred[1, :Exotic] == "win" && pred[1, :ROI] > cutoff
        minBaseAmount, multipleAmount = get_bet_mins(Exotics[pred[1, :Exotic]], betTypeInformation)
        first = parse(Int64, pred[1, :First])
        pays = get_bet_pays(Exotics[pred[1, :Exotic]], first, 0, race.odds)
        if pays==0 return 0 end
        wager = floor.((100/pays)/multipleAmount) * multipleAmount
        wager = wager<minBaseAmount? round.(minBaseAmount, 2): round.(wager, 2)
        println("$(pred[1, :ROIO])\tWin $(pred[1, :First])    \tProb $(pred[1, :ProbO])  \t\$$wager")
    elseif pred[1, :Exotic] == "ex" && pred[1, :ROI] > cutoff
        minBaseAmount, multipleAmount = get_bet_mins(Exotics[pred[1, :Exotic]], betTypeInformation)
#        pays = round.(1 / pred[1, :Prob], 2)
        first = parse(Int64, pred[1, :First])
        second = parse(Int64, pred[1, :Second])
        pays = get_bet_pays(Exotics[pred[1, :Exotic]], first, second, race.odds)
        if pays==0 return 0 end
        wager = floor.((200/pays)/multipleAmount) * multipleAmount
        wager = wager<minBaseAmount? round.(minBaseAmount, 2): round.(wager, 2)
        println("$(pred[1, :ROIO])\tEx  $(pred[1, :First]) $(pred[1, :Second])  \tProb $(pred[1, :ProbO])  \t\$$wager")
    elseif pred[1, :Exotic] == "tri" && pred[1, :ROI] > cutoff
        minBaseAmount, multipleAmount = get_bet_mins(Exotics[pred[1, :Exotic]], betTypeInformation)
        pays = round.(1 / pred[1, :Prob], 2)
        wager = floor.((400/pays)/multipleAmount) * multipleAmount
        wager = wager<minBaseAmount? round.(minBaseAmount, 2): round.(wager, 2)
        println("$(pred[1, :ROIO])\tTri $(pred[1, :First]) $(pred[1, :Second]) $(pred[1, :Third])\tProb $(pred[1, :ProbO])  \t\$$wager")
    end
    wager
end

@everywhere function MakeBet_Amtote(TEC::String, raceNumber::Int64, bets::DataFrame, races::Library)
    Exotics = Dict("win"=>"WN", "ex"=>"EX", "tri"=>"TR")
    betfile = ""
    temp = init_bets()

    for ii in 1:nrow(bets)
        bet = Exotics[bets[ii,:Exotic]]
        betfile *= "$ii|$TEC|$raceNumber|$(bets[ii,:Wager])|$bet|$(bets[ii,:First])"
		if bets[ii,:Second]>0 betfile *= ",$(bets[ii,:Second])" end
		if bets[ii,:Third]>0 betfile *= ",$(bets[ii,:Third])" end
		betfile *= "\n"
    end
    if betfile != ""
        betfile *= "\n"
		sock = races.sock
        result = write_socket(sock, "updateBalance\n")
        result = write_socket(sock, "updateBalance\n")
		write(sock, "submitBets $betfile")

		while (true)
			result = readline(sock)
		    if result=="-----ES--" break end
		    resfields = split(result, ['|'])
		    if length(resfields)<7
		        if length(result)>0 println("Received an invalid result: $result") end
		        continue
		    end
		    if parse(Int64, resfields[7]) == 0
		        ii = parse(Int64, resfields[1])
		        temp = [temp; bets[ii, :]]
		    else
		        println(result)
            end
		end
    end
    bets = temp
end

@everywhere function build_bets!(dateTime::DateTime, preds::DataFrame, TEC::String, raceNumber::Int64, races::Library)
    wagers_df = init_bets()
    source = live(races)? "SIM": "Log"
    track = races.Cards[TEC].Track

    for row in 1:nrow(preds)
        wager = valid_bet(preds[row, :], TEC, raceNumber, races)
        if wager > 0
            entry = wager_entry!(track, TEC, raceNumber, source, preds[row, :], wager, races)
            save_bets!(wagers_df, entry)
        end
    end
    if source == "SIM" wagers_df = MakeBet_Amtote(TEC, raceNumber, wagers_df, races) end
    wagers_old = races.Cards[TEC].Races[raceNumber].wagers
    wagers_df = nrow(wagers_old)>0? [wagers_old; wagers_df]: wagers_df
    races.Cards[TEC].Races[raceNumber].wagers = wagers_df
end

@everywhere function store_win_preds(TEC::String, preds::DataFrame, races::Library)
    programDate = races.Cards[TEC].CardInfo.programDate
	Indexes = find(preds[:Exotic] .== "win")
    winPreds = preds[find(preds[:Exotic] .== "win"), :]
    append_file(winPreds, programDate, "Preds", races.outExt)
end

@everywhere function store_forms(TEC::String, raceNumber::Int64, races::Library)
    programDate = races.Cards[TEC].CardInfo.programDate
    race = races.Cards[TEC].Races[raceNumber]
    append_file(race.form, programDate, "Forms", races.outExt)
end

@everywhere function map_preds!(preds::DataFrame, data, forms::DataFrame, stat::String)
    if stat=="ExR"
        indexes = find(preds[:Exotic] .== "ex")
    elseif stat=="TriR"
        indexes = find(preds[:Exotic] .== "tri")
    end
    values = data[1]
    pts = data[2]
    index = 1
    for ii in indexes
        preds[ii, :ROI] = values[index]
        preds[ii, :First] = forms[pts[index][1], :Head]
        preds[ii, :Second] = forms[pts[index][2], :Head]
        if stat=="TriR"
            preds[ii, :Third] = forms[pts[index][3], :Head]
        end
        index += 1
    end
    preds[indexes, :] = add_ROIO!(preds[indexes, :])
end

@everywhere function adjust_preds!(preds::DataFrame, track::String, raceNumber::Int64, races::Library)
    race = races.Cards[track].Races[raceNumber]
    forms = race.form
    dps = collect(1:size(race.form)[1])
    model = races.betEqns["WinR"].gaits["All"].gait["A"].model
    tmp = pred_model(forms, model, dps=dps)
    winIndexes = find(preds[:Exotic] .== "win")
    preds[winIndexes, :ROI] = tmp
    preds[winIndexes, :] = add_ROIO!(preds[winIndexes, :])
    eqn = races.betEqns["Win"].gaits["All"].gait["A"].eqn
    df = init_wagers(forms, dps, "Win", 1)
    xterms = get_xterms(eqn, forms, dps, df)
    xterms = race_normal!(forms, df, xterms, dp_subset=dps)
    preds[winIndexes, :Prob] = xterms
    preds[winIndexes, :] = add_ProbO!(preds[winIndexes, :])

    model = races.betEqns["ExR"].gaits["All"].gait["A"].model
    tmp = pred_model_dp_combos(forms, model, dps=dps)
    map_preds!(preds, tmp, forms, "ExR")

    model = races.betEqns["TriR"].gaits["All"].gait["A"].model
    tmp = pred_model_dp_combos(forms, model, dps=dps)
    map_preds!(preds, tmp, forms, "TriR")
end

@everywhere function store_bets!(dateTime::DateTime, track::String, raceNumber::Int64, races::Library)
    augment_forms!(track, raceNumber, races)
    Preds = predict_race_historical!(track, raceNumber, races)
    adjust_preds!(Preds, track, raceNumber, races)
    Preds = combine_entries(Preds, track, raceNumber, races)
    build_bets!(dateTime, Preds, track, raceNumber, races)
    store_win_preds(track, Preds, races)
    store_forms(track, raceNumber, races)
end

@everywhere function check_odds!(races::Library, track::String, raceNumber::Int64)
    race = races.Cards[track].Races[raceNumber]
    savedPools = race.savedCycleData.pools
    savedOdds = race.savedCycleData.odds
    savedProbs = race.savedCycleData.probs
    oldPools = race.oldCycleData.pools
    oldOdds = race.oldCycleData.odds
    oldProbs = race.oldCycleData.probs
    CycleData = race.CycleData
    Pools = CycleData.pools
    if isempty(savedPools)
        savedPools = oldPools
        savedOdds = oldOdds
        savedProbs = oldProbs
    end

    for ii in 1:length(oldProbs)
        betType = oldProbs[ii].betType
        for jj in 1:length(Pools)
            if betType == Pools[jj].betType
		        savedPoolChange = savedPools[jj].poolChange
		        poolTotal = (Pools[jj].poolTotal!="RF")?parse(Int64, Pools[jj].poolTotal):0
		        oldPoolTotal = (oldPools[jj].poolTotal!="RF")?parse(Int64, oldPools[jj].poolTotal):0
		        PoolChange = round.(((poolTotal - oldPoolTotal) / oldPoolTotal), 4)
		        if PoolChange >= savedPoolChange
		            savedPools[jj] = oldPools[jj]
		            savedPools[jj].poolChange = PoolChange
		            for kk in 1:length(oldOdds)
		                if betType == oldOdds[kk].betType
		                    length(savedOdds)<length(oldOdds)?push!(savedOdds,oldOdds[kk]):savedOdds[kk]=oldOdds[kk]
                        end
		            end
		            length(savedProbs)<length(oldProbs)?push!(savedProbs,oldProbs[ii]):savedProbs[ii]=oldProbs[ii]
		            break
		        end
		        break
            end
        end
    end
    races.Cards[track].Races[raceNumber].savedCycleData.programName = CycleData.programName
    races.Cards[track].Races[raceNumber].savedCycleData.race = CycleData.race
    races.Cards[track].Races[raceNumber].savedCycleData.source = CycleData.source
    races.Cards[track].Races[raceNumber].savedCycleData.pools = savedPools
    races.Cards[track].Races[raceNumber].savedCycleData.odds = savedOdds
    races.Cards[track].Races[raceNumber].savedCycleData.probs = savedProbs
end

@everywhere function adjust_prices(track::String, raceNumber::Int64, bet::DataFrame, prices::PriceRecord, rebates::DataFrame)
    rebate = rebates[1, Symbol(prices.poolID)]
    if prices.paid==0
        bet[1, :Wager] = 0
        return bet
    end
    bet[1, :aWager] = round.(bet[1, :Wager] - bet[1, :Wager] * rebate / 100, 2)
    ticket = split(prices.results, ['-'])
    first = "$(bet[1, :First])"
    second = "$(bet[1, :Second])"
    third = "$(bet[1, :Third])"
    fourth = "$(bet[1, :Fourth])"
    fifth = "$(bet[1, :Fifth])"
    paid = prices.paid
    baseValue = prices.baseValue
    results = prices.results
    Balance = round.(bet[1, :Wager] * paid / baseValue, 2)
    if (prices.poolID=="WN" && ticket[1]==first) ||
       (prices.poolID=="EX" && ticket[1]==first && ticket[2]==second) ||
       (prices.poolID=="TR" && ticket[1]==first && ticket[2]==second && ticket[3]==third)
		bet[1, :Balance] = Balance
		println("Cashed $(rebates[1, :Name]) Race $raceNumber $(bet[1, :Exotic]) $results \$$baseValue \$$paid \$$Balance")
    end
    bet
end

@everywhere function check_prices!(record::AmtCallbackData, bets::DataFrame, races::Library)
    poolIDs = Dict("win"=>"WN","ex"=>"EX","tri"=>"TR","sup"=>"SU","db"=>"DB","p3"=>"P3","p4"=>"P4","p5"=>"P5","p6"=>"P6","e5"=>"E5")
    track = record.obj.programName
    raceNumber = record.obj.race
    store_final_odds!(record.date, track, raceNumber, races)
    race = races.Cards[track].Races[raceNumber]
    prices_df = init_prices()
    source = live(races)? "SIM": "Log"
    bets[:Delete] = 0
    prices = record.obj.prices
    rebates = races.Rebates[findfirst(races.Rebates[:TEC], track), :]
    programDate = races.Cards[track].CardInfo.programDate

    for ii in 1:length(prices)
        if prices[ii].poolID in values(poolIDs) && prices[ii].baseValue > 0
            price_entry!(race.rnum, programDate, source, prices_df, prices[ii])
            println("$(prices[ii].poolID) base $(prices[ii].baseValue) result $(prices[ii].results) paid $(prices[ii].paid)")
        end
        for jj in 1:nrow(bets)
            exotic = bets[jj, :Exotic]
            if prices[ii].poolID == poolIDs[exotic]
                bets[jj, :] = adjust_prices(track, raceNumber, bets[jj, :], prices[ii], rebates)
                if bets[jj, :Wager] == 0 bets[jj, :Delete] = 1 end
            end
        end
    end
    append_file(prices_df, programDate, "Prices", races.outExt)
    if nrow(bets) == 0
        println("No bets for $track $raceNumber")
        return
    end
    bets = bets[(bets[:Delete].==0), :]
    delete!(bets, [:Delete])
    append_file(bets, programDate, "Bets", races.outExt)
    races.Cards[track].Races[raceNumber].wagers = init_bets()
end

@everywhere function check_late_scratches!(scratches::String, bets::DataFrame)
    scratches = split(scratches, [','])
    temp = copy(bets)
    temp[:Delete] = 0

    for ii in 1:length(scratches)
        scr = parse(Int64, scratches[ii])
        for jj in 1:nrow(bets)
            if bets[jj,:First]==scr||bets[jj,:Second]==scr||bets[jj,:Third]==scr||bets[jj,:Fourth]==scr||bets[jj,:Fifth]==scr
                println("Found a late scratch: $scr in $(bets[jj,:Exotic])")
              	temp[jj, :Delete] = 1
            end
        end
    end
    temp = temp[(temp[:Delete].==0), :]
    delete!(temp, [:Delete])
    bets = temp
end

@everywhere function scratch_horse!(races::Library, track::String, raceNumber::Int64, head::String)
    programLongName = races.Cards[track].CardInfo.programLongName
    starters = races.Cards[track].Races[raceNumber].ItpData.starters
    found = false

    for starter in starters
        if starter.programNumber==head
            println("Scratch the $head in Race $raceNumber at $programLongName")
            filter!(e->e!=starter, starters)
            found = true
            break
        end
    end
    found
end

@everywhere function write_file(dbname::String)
	dir = ENV["DATADIR"]
    println("Writing $dbname")
    flush(STDOUT)
    con = DB_connect()
    command = "SELECT * from $dbname"
    table = MySQL.query(con, command, DataFrame)
    MySQL.disconnect(con)
    if dbname == "Races" table[:Date] = map(x->parse(Int64, replace("$x", "-", "")), table[:Date]) end
    file = dbname * ".csv"
    println("Writing $file")
	CSV.write(dir * file, table)
end

@everywhere function redo_cards!(races::Library)
	df = Dates.DateFormat("yyyymmdd")
	date = parse(Int64, (Dates.format(races.firstPost, df)))
    day = Dates.dayname(races.firstPost)
    println("Redoing all cards for $day $date, found $(races.changes) changes")
    flush(STDOUT)
    write_file("WWLines")
    flush(STDOUT)
    write_file("Races")
    flush(STDOUT)
    write_file("Rebates")
    flush(STDOUT)
    write_file("Vigs")
    flush(STDOUT)
#    write_file("TrackNames")
#    flush(STDOUT)
    dir = "../horses/"
	run(`julia "$(dir)train_drive_post_ave.jl" $date $date`)
    flush(STDOUT)
	run(`julia "$(dir)track_ratings.jl" $date $date`)
    flush(STDOUT)
	run(`julia "$(dir)build_form.jl" $date $date`)
    flush(STDOUT)

	dir = ENV["DATADIR"] * "$date/"
	rm(dir * "Forms_df.csv")
    races.forms, delays = get_daily_input(date)
    races.changes = 0

    for k in keys(races.Cards)
        races.Cards[k].Races = add_races!(races.Cards[k].Track, races.forms, k, races)
    end
end

@everywhere function redo_race!(rnum::Int64, races::Library, track::String, raceNumber::Int64)
	horses = get_latest_entries(rnum)
    if nrow(horses)==0 return end
	factors = strip_prefix!(load_factor_fn())
	date = horses[1,:Date]
	data = get_race_day(date)
	old = clean_old(data["cards"], date)
    old[:TS] = "NA"
	data["cards"] = [horses; old]
	build_race_form!(data, horses, factors)
    races.forms, delays = get_daily_input(date)
    if races.Cards[track].CardInfo.currentRace==raceNumber races.Cards[track].CardInfo.posted = 0 end
    races.Cards[track].Races[raceNumber].form = races.forms[find(races.forms[:Rnum] .== rnum), :]
end

@everywhere function check_selections!(track::String, raceNumber::Int64, races::Library)
    race = races.Cards[track].Races[raceNumber]
    forms = races.forms
    changes = false
    rnum = races.Cards[track].Races[raceNumber].rnum
    if rnum == 0 return changes end
    rIndexes = find(forms[:Rnum] .== rnum)

    for horse in race.ItpData.starters
        hIndexes = fast_intersect(rIndexes, find(forms[:Hnum] .== horse.hnum))
        if !isempty(hIndexes)
            if check_details(rnum, horse, forms, hIndexes[1]) changes = true end
        end
    end
    dps = collect(1:size(race.form)[1])
    if length(dps) > length(race.ItpData.starters)
        changes = true

	    for ii in 1:length(dps)
	        head = forms[dps[ii], :Head]
	        found = false
	        for horse in race.ItpData.starters
	            if (head == horse.programNumber)||(lowercase(head) == horse.programNumber) found = true end
	        end
	        if !found
	            changes = true
	            println("Found $head in the form and not in $track Race $raceNumber")
	            change_form_DB(rnum, forms[dps[ii], :Hnum])
	        end
	    end
    end
	flush(STDOUT)
	changes
end
