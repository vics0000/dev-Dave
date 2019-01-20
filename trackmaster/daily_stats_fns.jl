# using Distributed# 0.7 code
# daily_stats_fns.jl

#################################################################
# Driver, Trainer, and Post averages functions

@everywhere function win_stats(all_stats::Stats, tat_stats::Stats, cnt=10)
	starts = tat_stats.starts
	wins = tat_stats.wins
	earn = tat_stats.earnswin
	balance = tat_stats.winpays
	allBalance = tat_stats.pays
	roi = starts>0 ? round.((balance + wins - starts) / starts, 2) : 0
	payw = wins>0 ? round.((balance + wins) / wins, 2) : 0
	pay = starts>0 ? round.((allBalance + starts) / starts, 2) : 0
	val = starts>0 ? round.(payw * wins / starts, 3) : 0
	earn = starts>0 ? round.(earn / starts, 2) : 0
	adjustedCount = starts>0 ? round.(wins / starts, 2) : 0
	wins = starts>0 ? round.(wins / starts,3) : 0
	if starts < cnt
		allStarts = all_stats.starts==0 ? 1 : all_stats.starts
		allWins = all_stats.wins==0 ? 1 : all_stats.wins
		allEarnings = round.(all_stats.earnswin / allStarts, 2)
		allWinBalance = all_stats.winpays
		overallBalance = all_stats.pays
		allRoi = (allWinBalance + allWins - allStarts) / allStarts
		allPay = (overallBalance + allStarts) / allStarts
		allPayW = (allWinBalance + allWins) / allWins
		allVal = round.(allPayW * allWins / allStarts,3)
		avePos = allWins / allStarts
		allWins = round.(allWins / allStarts,3)
		roi = round.((roi * starts + allRoi * (cnt - starts)) / cnt, 2)
		pay = round.((pay * starts + allPay * (cnt - starts)) / cnt, 2)
		payw = round.((payw * starts + allPayW * (cnt - starts)) / cnt, 2)
		val = round.((val * starts + allVal * (cnt - starts)) / cnt, 3)
		wins = round.((wins * starts + allWins * (cnt - starts)) / cnt, 3)
		earn = round.((earn * starts + allEarnings * (cnt - starts)) / cnt, 2)
		adjustedCount = round.((adjustedCount * starts + avePos * (cnt - starts)) / cnt, 2)
	end
	[wins, roi, payw, pay, val, earn, adjustedCount]
end

@everywhere function pos_stats(all_stats::Stats, tat_stats::Stats, pos::Int64, cnt=10)
	starts = tat_stats.starts
	posCnt = pos==2 ? tat_stats.places : tat_stats.shows
	earn = pos==2 ? tat_stats.earnsplace : tat_stats.earnsshow
	balance = pos==2 ? tat_stats.placepays : tat_stats.showpays
	roi = starts>0 ? round.((balance + posCnt - starts) / starts, 2) : 0
	payp = posCnt>0? round.((balance + posCnt) / posCnt, 2) : 0
	val = starts>0 ? round.(payp * posCnt / starts, 3) : 0
	earn = starts>0 ? round.(earn / starts, 2) : 0
	adjustedCount = starts>0 ? round.(posCnt / starts, 2) : 0
	posCount = starts>0 ? round.(posCnt / starts,3): 0
	if starts < cnt
		allStarts = all_stats.starts==0 ? 1 : all_stats.starts
		allPos = pos==2 ? all_stats.places : all_stats.shows
		allEarnings = pos==2 ? all_stats.earnsplace : all_stats.earnsshow
		allEarnings = round.(allEarnings / allStarts, 2)
		allBalance = pos==2 ? all_stats.placepays : all_stats.showpays
		allPayPos = (allBalance + allPos) / allPos
		allVal = round.(allPayPos * allPos / allStarts,3)
		allRoi = (allBalance + allPos - allStarts) / allStarts
		allPos = round.(allPos / allStarts, 3)
		roi = round.((roi * starts + allRoi * (cnt - starts)) / cnt, 2)
		payp = round.((payp * starts + allPayPos * (cnt - starts)) / cnt, 2)
		val = round.((val * starts + allVal * (cnt - starts)) / cnt, 3)
		posCount = round.((posCount * starts + allPos * (cnt - starts)) / cnt, 3)
		earn = round.((earn * starts + allEarnings * (cnt - starts)) / cnt, 2)
		avePos = posCnt / allStarts
		adjustedCount = round.((adjustedCount * starts + avePos * (cnt - starts)) / cnt, 2)
	end
	[posCount, roi, payp, val, earn, adjustedCount]
end

@everywhere function calc_post_stats(track::String, post::Int64, gait::String, stats::StatsTables)
	if get(stats.tracks, track, 0)==0
		stats.tracks[track] = Dict{String, Dict{Int64,Stats}}()
		stats.tracks[track][gait] =  Dict{Int64,Stats}()
		stats.tracks[track][gait][post] =  Stats()
	elseif get(stats.tracks[track], gait, 0)==0
		stats.tracks[track][gait] =  Dict{Int64,Stats}()
		stats.tracks[track][gait][post] =  Stats()
	elseif get(stats.tracks[track][gait], post, 0)==0
		stats.tracks[track][gait][post] =  Stats()
	end
	tat_stats = stats.tracks[track][gait][post]
	all_stats = stats.catagories["overall"]["overall"]
	win = win_stats(all_stats, tat_stats)
	place = pos_stats(all_stats, tat_stats, 2)
	show = pos_stats(all_stats, tat_stats, 3)
	mps = round((win[6] + place[5] + show[5]), 2)
	ave = round((win[7] * 9 + place[6] * 5 + show[6] * 3) / 9, 3)
	[track,post,gait,tat_stats.starts,win[1],place[1],show[1],win[2],place[2],show[2],win[5],place[4],show[4],
		win[4],win[3],place[3],show[3],ave,mps]
end

@everywhere function calc_stats(stat::String, tat::String, stats::StatsTables, cut=10)
	if get(stats.catagories, stat, 0)==0
		stats.catagories[stat] = Dict{String,Stats}()
		stats.catagories[stat][tat] = Stats()
	elseif get(stats.catagories[stat], tat, 0)==0
		stats.catagories[stat][tat] = Stats()
	end
	if get(stats.catagories, "overall", 0)==0
		stats.catagories["overall"] = Dict{String,Stats}()
		stats.catagories["overall"]["overall"] = Stats()
	end
	tat_stats = stats.catagories[stat][tat]
	all_stats = stats.catagories["overall"]["overall"]
	win = win_stats(all_stats, tat_stats, cut)
	place = pos_stats(all_stats, tat_stats, 2, cut)
	show = pos_stats(all_stats, tat_stats, 3, cut)
	mps = round.((win[6] + place[5] + show[5]), 2)
	ave = round.((win[7] * 9 + place[6] * 5 + show[6] * 3) / 9, 3)
	[tat,tat_stats.starts,win[1],place[1],show[1],win[2],place[2],show[2],win[5],place[4],show[4],
		win[4],win[3],place[3],show[3],ave,mps]
end

@everywhere function driver_stats(date::Int64, roster, stats::StatsTables)
	drivers = keys(roster["drivers"])
	aves = init_aves()

    for driver in drivers
		detail = calc_stats("driver", driver, stats)
        push!(aves, detail)
    end
	dir = ENV["DATADIR"] * "dailies/$date/"
    CSV.write(dir*"Drivers.csv", aves)
end

@everywhere function trainer_stats(date::Int64, roster, stats::StatsTables)
	trainers = keys(roster["trainers"])
	aves = init_aves()

    for trainer in trainers
		detail = calc_stats("trainer", trainer, stats, 6)
        push!(aves, detail)
    end
	dir = ENV["DATADIR"] * "dailies/$date/"
    CSV.write(dir*"Trainers.csv", aves)
end

@everywhere function horse_stats(date::Int64, roster, stats::StatsTables)
	horses = keys(roster["horses"])
	aves = init_aves()

    for horse in horses
		detail = calc_stats("horse", horse, stats, 4)
        push!(aves, detail)
    end
	dir = ENV["DATADIR"] * "dailies/$date/"
    CSV.write(dir*"Horses.csv", aves)
end

@everywhere function track_stats(date::Int64, roster, stats::StatsTables)
	tracks = keys(roster["tracks"])
    aves = init_posts()

    for track in(tracks)
		gaits = keys(roster["tracks"][track])
        for gait in gaits
			posts = keys(roster["tracks"][track][gait])
            for post in posts
				detail = calc_post_stats(track, post, gait, stats)
                push!(aves, detail)
            end
        end
    end
	dir = ENV["DATADIR"] * "dailies/$date/"
    CSV.write(dir*"Posts.csv", aves)
end

@everywhere function calc_track_rating!(track::String, stats::StatsTables)
	if get(stats.catagories, "track", 0)==0
		stats.catagories["track"] = Dict{String,Stats}()
		stats.catagories["track"][track] = Stats()
	elseif get(stats.catagories["track"], track, 0)==0
		stats.catagories["track"][track] = Stats()
	end
	tat_stats = stats.catagories["track"][track]
	starts = tat_stats.races
	if starts==0 tat_stats.rating = 120.0 end
	rating = round.(tat_stats.rating, 1)
	if starts>0 && starts<20
		overallRating = stats.catagories["overall"]["overall"].rating
		rating = round((rating * starts + overallRating * (20 - starts)) / 20, 1)
	end
	[track, starts, rating]
end

@everywhere function track_ratings(date::Int64, roster, stats::StatsTables)
	tracks = keys(roster["tracks"])
	ratings = init_ratings()

    for track in tracks
		detail = calc_track_rating!(track, stats)
        push!(ratings, detail)
    end
	dir = ENV["DATADIR"] * "dailies/$date/"
    CSV.write(dir*"Tracks.csv", ratings)
end

@everywhere function build_card_dets(date::Int64, races::DataFrame, day_dps::Array{Int64})
	dir = ENV["DATADIR"] * "dailies/$date/"
	rm(dir*"Cards.csv", force=true)
	horses = unique(races[day_dps, :Hnum])
	entries = Dict{String, Int64}()
	drivers = Dict{String, Int64}()
	trainers = Dict{String, Int64}()
	tracks = Dict{String, Dict{String, Dict{Int64, Int64}}}()

    for horse in horses
		details = get_tattoo_history(date, horse)
		entries[horse] = 0
		for ii in 1:nrow(details)
			entries[horse] += 1
			if get(tracks, details[ii, :track], 0)==0
				tracks[details[ii, :track]] = Dict{String, Dict{Int64, Int64}}()
				tracks[details[ii, :track]][details[ii, :Gait]] = Dict{Int64, Int64}()
				tracks[details[ii, :track]][details[ii, :Gait]][details[ii, :Post]] = 0
			elseif get(tracks[details[ii, :track]], details[ii, :Gait], 0)==0
				tracks[details[ii, :track]][details[ii, :Gait]] = Dict{Int64, Int64}()
				tracks[details[ii, :track]][details[ii, :Gait]][details[ii, :Post]] = 0
			elseif get(tracks[details[ii, :track]][details[ii, :Gait]], details[ii, :Post], 0)==0
				tracks[details[ii, :track]][details[ii, :Gait]][details[ii, :Post]] = 0
			end
			tracks[details[ii, :track]][details[ii, :Gait]][details[ii, :Post]] += 1
			if get(drivers, details[ii, :Dnum], 0)==0 drivers[details[ii, :Dnum]] = 0 end
			drivers[details[ii, :Dnum]] += 1
			if get(trainers, details[ii, :Tnum], 0)==0 trainers[details[ii, :Tnum]] = 0 end
			trainers[details[ii, :Tnum]] += 1
		end
    end
	Dict("horses"=>entries, "drivers"=>drivers, "trainers"=>trainers, "tracks"=>tracks)
end

@everywhere function add_to_tattoo(today::DataFrame)

	for ii in 1:nrow(today)
		hash = [today[ii,:Date], today[ii,:Track], today[ii,:Race], today[ii,:Hnum],
				today[ii,:Rnum], today[ii,:Gait], today[ii,:Post], today[ii,:Tnum], today[ii,:Dnum]]
		update_tattoo!(hash)
	end
end

@everywhere function update_stat_details(update::String, stat::String, tat::String, races::DataFrame,
										sdps::Array{Int64},  wps, stats::StatsTables)
	add = update=="add" ? 1 : -1
	day = races[sdps, :]
	w_dps = fast_intersect(sdps, wps[1])
	p_dps = fast_intersect(sdps, wps[2])
	s_dps = fast_intersect(sdps, wps[3])
	wp_dps = [w_dps; p_dps]
	wps_dps = [wp_dps; s_dps]
	tat_stats = stats.catagories[stat][tat]
	tat_stats.starts += nrow(day) * add
	tat_stats.wins += length(w_dps) * add
	tat_stats.places += length(p_dps) * add
	tat_stats.shows += length(s_dps) * add
	tat_stats.earnswin += sum(races[w_dps, :Earns]) * add
	tat_stats.earnsplace += sum(races[p_dps, :Earns]) * add
	tat_stats.earnsshow += sum(races[s_dps, :Earns]) * add
	tat_stats.winpays += sum(races[w_dps, :Pays]) * add
	tat_stats.placepays += sum((races[wp_dps, :Ppay] - 2)/2) * add
	tat_stats.showpays += sum((races[wps_dps, :Spay] - 2)/2) * add
	tat_stats.placepays = round.(tat_stats.placepays, 2)
	tat_stats.showpays = round.(tat_stats.showpays, 2)
	tat_stats.pays += sum(day[:Pays]) * add
end

@everywhere function update_track_details(update::String, track::String, gait::String, post::Int64,
										races::DataFrame, sdps::Array{Int64},  wps, stats::StatsTables)
	add = update=="add" ? 1 : -1
	day = races[sdps, :]
	w_dps = fast_intersect(sdps, wps[1])
	p_dps = fast_intersect(sdps, wps[2])
	s_dps = fast_intersect(sdps, wps[3])
	wp_dps = [w_dps; p_dps]
	wps_dps = [wp_dps; s_dps]
	tat_stats = stats.tracks[track][gait][post]
	tat_stats.starts += nrow(day) * add
	tat_stats.wins += length(w_dps) * add
	tat_stats.places += length(p_dps) * add
	tat_stats.shows += length(s_dps) * add
	tat_stats.earnswin += sum(races[w_dps, :Earns]) * add
	tat_stats.earnsplace += sum(races[p_dps, :Earns]) * add
	tat_stats.earnsshow += sum(races[s_dps, :Earns]) * add
	tat_stats.winpays += sum(races[w_dps, :Pays]) * add
	tat_stats.placepays += sum((races[wp_dps, :Ppay] - 2)/2) * add
	tat_stats.showpays += sum((races[wps_dps, :Spay] - 2)/2) * add
	tat_stats.pays += sum(day[:Pays]) * add
end

@everywhere function update_track_ratings(update::String, track::String,
										day::DataFrame, stats::StatsTables)
	add = update=="add" ? 1 : -1
	tat_stats = stats.catagories["track"][track]
	tat_stats.races += nrow(day) * add
	tat_stats.times += sum(day[:Rtime]) * add
	tat_stats.rating = round.((tat_stats.times / tat_stats.races), 2)
end

@everywhere function add_VCB_to_races(races::DataFrame, r_inds, racelines::DataFrame, date::Int64,
									day_dps::Array{Int64}, stats::StatsTables, roster)
	dps = get_date_dps(r_inds, date, date, true)
	BVCraces = races[dps, :]
	today = racelines[day_dps, :]
	track = today[1, :Track]
	rnum = today[1, :Rnum]
	babies = 0
	horses = 0
	cnt = 0
	times = 0

	for ii in 1:nrow(today)
		babies += roster["horses"][today[ii, :Hnum]]<4 ? 1 : 0
		horses += 1
		if rnum != today[ii, :Rnum] || ii == nrow(today)
			index = findfirst(BVCraces[:Rnum], rnum)
			if BVCraces[index, :Distance]==1.0
				cnt += 1
				times += BVCraces[index, :Rtime]
			end
			if babies/horses>=.333 BVCraces[index, :Baby] = 1 end
			babies = 0
			horses = 0
			rnum = today[ii, :Rnum]
		end
		if track != today[ii, :Track] || ii == nrow(today)
			if cnt>0
				times = times / cnt
				rating = stats.catagories["track"][track].rating
				BVCraces[find(BVCraces[:Track] .== track), :VC] = round.(rating - times, 3)
			end
			cnt = 0
			times = 0
			track = today[ii, :Track]
		end

	end
	CSV.write(ENV["DATADIR"]*"races_BVC.csv", BVCraces; append=true)
end

@everywhere function update_stats(races::DataFrame, date::Int64, update::String,
								day_dps::Array{Int64}, stats::StatsTables)
	add = update=="add" ? 1 : -1
	day = races[day_dps, :]
	day = day[find(day[:Pays] .> 0), :]
	day = day[find(day[:Post] .> 0), :]
	wps = [find(day[:Fpos] .== 1), find(day[:Fpos] .== 2), find(day[:Fpos] .== 3)]
	indexes = index_races(day)
	dps = get_date_dps(indexes["dates"], date, date, true)
	update_stat_details(update, "overall", "overall", day, dps, wps, stats)
	all_stats = stats.catagories["overall"]["overall"]
	drivers = unique(day[:Dnum])
	d_indexes = indexes["drivers"]
	println("for driver in drivers")
	for driver in drivers
		if driver!="-" update_stat_details(update, "driver", driver, day, d_indexes[driver], wps, stats) end
    end
	trainers = unique(day[:Tnum])
	t_indexes = indexes["trainers"]
	println("for trainer in trainers")
	for trainer in trainers
		if trainer!="-" update_stat_details(update, "trainer", trainer, day, t_indexes[trainer], wps, stats) end
    end
	horses = unique(day[:Hnum])
	h_indexes = indexes["horses"]
	println("for horse in horses")
	for horse in horses
		update_stat_details(update, "horse", horse, day, h_indexes[horse], wps, stats)
    end
	tracks = unique(day[:Track])
	t_indexes = indexes["tracks"]
	g_indexes = indexes["gaits"]
	p_indexes = indexes["posts"]
	println("track in tracks")
    for track in tracks
		temp = day[t_indexes[track], :]
		gaits = unique(temp[:Gait])
        for gait in gaits
			tg_dps = fast_intersect(t_indexes[track], g_indexes[gait])
			posts = unique(day[tg_dps, :Post])
            for post in posts
				pdps = fast_intersect(tg_dps, p_indexes[post])
				update_track_details(update, track, gait, post, day, pdps, wps, stats)
            end
        end
    end
	unique!(day, :Rnum)
	day = day[find(day[:Cond] .== "FT"), :]
	day = day[find(day[:Distance] .== 1.0), :]
	tracks = unique(day[:Track])
	inds = index_races_by_col(day, :Track)
	println("track in tracks-ratings\n")
	for track in(tracks)
		temp = day[inds[track], :]
		update_track_ratings(update, track, temp, stats)
	end
	all_stats.races += nrow(day) * add
	all_stats.times += sum(day[:Rtime]) * add
	all_stats.rating = round.((all_stats.times / all_stats.races), 2)
end

@everywhere function check_stats(races::DataFrame, r_inds, racelines::DataFrame, date::Int64,
								day_dps::Array{Int64}, indexes, stats::StatsTables, roster)
	println("Updating $date")
#	sd = get_startdate(date)
	sd = get_startdate(date, 6)
	if stats.start==0 stats.start = date end
	dps = get_date_dps(indexes["dates"], date, date, true)
	index = findfirst(racelines[dps, :R], "T")
	if date>stats.current && index>0
		stats.current = date
		update_stats(racelines, date, "add", day_dps, stats)
		add_VCB_to_races(races, r_inds, racelines, date, dps, stats, roster)
	end
	if sd>stats.start
		stats.start = sd
		sd_dps = get_date_dps(indexes["dates"], sd, sd, true)
		e_dps = indexes["entries"]["T"]
		sd_dps = fast_intersect(sd_dps, e_dps)
		update_stats(racelines, date, "cut", sd_dps, stats)
	end
	save_stats_tables(stats)
end
