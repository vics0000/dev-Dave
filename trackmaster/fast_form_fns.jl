# using Distributed # 0.7 code
# fast_form_fns.jl

#################################################################
# General data manipualtion functions

# Suspect a horse off for more than 15 days
@everywhere function layoff(date::Int64)
	df = Dates.DateFormat("yyyymmdd")
	parse(Int64, (Dates.format(((Date("$date",df)) - Dates.Day(15)), df)))
end

# mark bad data
@everywhere function markup_valid!(lines::DataFrame)

	for ii in 1:nrow(lines)
		if lines[ii,:Qt]<=0||lines[ii,:Ht]<=0||lines[ii,:Tt]<=0||lines[ii,:Ft]<=0 lines[ii, :Valid] = false end
		if lines[ii,:Qpos]<=0||lines[ii,:Hpos]<=0||lines[ii,:Tpos]<=0||lines[ii,:Fpos]<=0 lines[ii, :Valid] = false end
		if lines[ii,:Fpos]<=0 lines[ii, :Valid] = false end
	end
end

# seperate out driver, trainer, track changes for speed
@everywhere function markup_changes!(lines::DataFrame, inds::Dict, horses::DataFrame, stat::Symbol)
	lines[:Change] = false

	for ii in 1:nrow(horses)
		l_inds = get(inds, horses[ii, :Hnum], 0)==0 ? [] : inds[horses[ii, :Hnum]]
		for index in l_inds
			if lines[index, stat] != horses[ii, stat] lines[index, :Change] = true end
		end
	end
end

# Add latest driver, trainer, or track change
@everywhere function markup_lines!(lines::DataFrame, inds::Dict, horses::DataFrame, stat::Symbol)
	markup_valid!(lines)
	markup_changes!(lines, inds, horses, stat)

	for ii in 1:nrow(horses)
		l_inds = get(inds, horses[ii, :Hnum], 0)==0 ? [] : inds[horses[ii, :Hnum]]
		off = layoff(horses[ii, :Date])
		for index in l_inds
			if lines[index, :Date] < off lines[index, :Off] = true end
			off = layoff(lines[index, :Date])
		end
	end
end

@everywhere function change_to_STD(table::DataFrame, stats::Array{Float64}, horses::DataFrame)
	if length(stats) <= 1
		if length(stats)==0 return stats end
		stats[abs.(stats).>0.0] = stats[abs.(stats).>0.0] ./ abs.(stats[abs.(stats).>0.0])
		return stats
    end
    changeSTD = std(table[:, Symbol(horses[1, :Stat])])
    stats = round.((stats / changeSTD), 4)
    stats[abs.(stats).>2] = 2 * stats[abs.(stats).>2] ./ abs.(stats[abs.(stats).>2])
    stats
end

@everywhere function history_to_STD(table::DataFrame, stats::Array{Float64}, horses::DataFrame)
	if length(stats) <= 1
		if length(stats)==0 return stats end
		stats[abs.(stats).>0.0] = stats[abs.(stats).>0.0] ./ abs.(stats[abs.(stats).>0.0])
		return stats
    end
	col = Symbol(horses[1, :Stat])
	histSTD = std(table[:, col])
	if histSTD==0 histSTD = 1 end
	histMean = mean(table[:, col])
	stats = round.(((stats - histMean) / histSTD), 4)
	stats[abs.(stats).>2] = 2 * stats[abs.(stats).>2] ./ abs.(stats[abs.(stats).>2])
	stats
end

@everywhere function stats_to_STD(stats::Array{Float64})::Array{Float64,1}
	if length(stats) <= 1
		if length(stats)==0 return stats end
		stats[abs.(stats).>0.0] = stats[abs.(stats).>0.0] ./ abs.(stats[abs.(stats).>0.0])
		return stats
    end
	STD = std(stats)
	stats = STD==0 ? zeros(stats) : round.(((stats - mean(stats)) / STD), 4)
end

@everywhere function stat_to_O(stats::Array{Float64})
	temp = sort(stats)

	for ii in 1:length(stats)
		for jj in 1:length(stats)
			if temp[jj]==stats[ii]
				stats[ii] = jj
				break
			end
		end
	end
    stats
end

@everywhere function STD_to_O(statsin::Array{Float64})
	stats = copy(statsin)
	if length(stats) <= 1
		if length(stats)==0 return stats end
		stats[abs.(stats).>0.0] = stats[abs.(stats).>0.0] ./ abs.(stats[abs.(stats).>0.0])
		return stats
    end
	temp = sort(stats)

	for ii in 1:length(stats)
		for jj in 1:length(temp)
			if temp[jj]==stats[ii]
				stats[ii] = jj
				break
			end
		end
	end
	stats = stats_to_STD(stats)
end

# Remove line if the trainer changed in the last race
@everywhere function cleanup_lines(lines::DataFrame, horses::DataFrame, ftype="time")
	if nrow(lines) == 0 return lines end
	dps = Int64[]

	for ii in 1:nrow(horses)
		indexes = find(lines[:Hnum] .== horses[ii, :Hnum])
		for jj in 1:length(indexes)
			if jj==1 && (lines[indexes[1], :Change] || lines[indexes[1], :Off]) continue end
			if ftype=="time" && !lines[indexes[jj], :Valid] continue end
			dps = [dps; indexes[jj]]
		end
	end
	lines[dps, :]
end

# Build and return the raw statistic
@everywhere function get_stat!(factor::String, data::Dict, horses::DataFrame, stat::String, lines::DataFrame, inds::Dict)
	lines[:Stat] = horses[1, :Stat]
	calculate_stat!(data, lines, inds, horses, stat)
end

# Pull the number of racelines from the factor
@everywhere function get_line_count(factor::String)
	parse(Int64, factor[end])
end

# Returns the raceline datapoints for the horses in a race
function get_lines_dps(data::Dict, horses::DataFrame, cnt::Int64=6)
	all_dps = Int64[]

	for horse in horses[:Hnum]
		indexes = data["indexes"]["cards"][horse]
		if length(indexes)>cnt+1 indexes = indexes[1:cnt+1] end
		indexes = length(indexes)>1 ? indexes[2:end] : []
		append!(all_dps, indexes)
	end
	all_dps
end

# Returns the racelines for the horses in a race
function get_lines(data::DataFrame, horses::DataFrame, num::Int64=6)
	lines = copy(data)
	allIndexes = Int64[]
	index = 0

	for horse in horses[:Hnum]
		for ii in 1:num
			index = findnext(lines[:Hnum], horse, index+1)
			index==0 ? break : append!(allIndexes, index)
		end
	end
	lines[allIndexes, :]
end

# Returns the horse's best trend
@everywhere function best_trend(x, y, z)
    temp = Float64[]

	for i in 1:length(x)
		if (x[i] != 0 & ((x[i] >= y[i]) | (y[i] == 0)) & ((x[i] >= z[i]) | (z[i] == 0)))
		    push!(temp, x[i])
		elseif (y[i] != 0 & ((y[i] >= x[i]) | (x[i] == 0)) & ((y[i] >= z[i]) | (z[i] == 0)))
		    push!(temp, y[i])
		elseif (z[i] != 0 & ((z[i] >= x[i]) | (x[i] == 0)) & ((z[i] >= y[i]) | (y[i] == 0)))
		    push!(temp, z[i])
		else push!(temp, 0) end
	end
	temp
end

# True for change in the specified race and not since
@everywhere function valid_change(lines::DataFrame, race::Int64)

	for ii in 1:nrow(lines)
		if lines[ii, :Change] && ii < race return false end
		if lines[ii, :Change] && ii == race return true end
	end
	false
end

# True for layoff/no start in the specified race and not since
@everywhere function valid_off(lines::DataFrame, race::Int64)

	for ii in 1:nrow(lines)
		if lines[ii, :Off] && ii < race return false end
		if lines[ii, :Off] && ii == race return true end
	end
	false
end

# Return the requested stat if layoff found
@everywhere function stat_history!(lines::DataFrame, inds::Dict, horses::DataFrame)
	temp = DataFrame(Post=Int64[], Hnum=String[], Calc=Float64[])
	CR = horses[1, :CR]

	for ii in 1:nrow(horses)
		l_inds = get(inds, horses[ii, :Hnum], 0)==0 ? [] : inds[horses[ii, :Hnum]]
		if horses[ii, :Check]=="F"
			horses[ii, :Check] = "X"
			if length(l_inds) > 0
				if valid_off(lines[l_inds, :], CR)
					push!(temp, (horses[ii,:Post], horses[ii,:Hnum], horses[ii,:Calc]))
					horses[ii, :Check] = "T"
				end
				if length(l_inds)==(CR - 1)
					push!(temp, (horses[ii,:Post], horses[ii,:Hnum], horses[ii,:Calc]))
					horses[ii, :Check] = "T"
				end
			elseif CR==1
				push!(temp, (horses[ii,:Post], horses[ii,:Hnum], horses[ii,:Calc]))
				horses[ii, :Check] = "T"
			end
		elseif horses[ii, :Check]=="T"
			push!(temp, (horses[ii,:Post], horses[ii,:Hnum], horses[ii,:Calc]))
		end
	end
	temp
end

# Return the change in a requested stat
@everywhere function stat_change!(lines::DataFrame, inds::Dict, horses::DataFrame)
	temp = DataFrame(Post=Int64[], Hnum=String[], Calc=Float64[])
	num = horses[1, :CR]

	for ii in 1:nrow(horses)
		l_inds = get(inds, horses[ii, :Hnum], 0)==0 ? [] : inds[horses[ii, :Hnum]]
		if horses[ii, :Check]=="F"
			horses[ii, :Check] = "X"
			if valid_change(lines[l_inds, :], num)
				push!(temp, (horses[ii,:Post], horses[ii,:Hnum], lines[l_inds[num], :Calc]))
				horses[ii, :Check] = "T"
			end
		elseif horses[ii, :Check]=="T"
			push!(temp, (horses[ii,:Post], horses[ii,:Hnum], lines[l_inds[num], :Calc]))
		end
	end
	temp
end

# Build and return the raw change
@everywhere function get_change!(factor::String, data::Dict, horses::DataFrame, stat::String, lines::DataFrame, inds::Dict)
	lines[:Stat] = horses[1, :Stat]
	calculate_stat!(data, lines, inds, horses, stat)
end

# False if there has been a trainer change, time off or not enough races
@everywhere function valid_trend(cards::DataFrame, l_inds::Array{Int64}, t_inds::Array{Int64})
	if length(l_inds)<t_inds[end] return false end

	for i in t_inds
		if cards[l_inds[i], :Change] || cards[l_inds[i], :Off] return false end
	end
	true
end

# Trends are last 3, 2 or 2nd last 2 races
@everywhere function trend_indexes(stat::String)
	inds = Dict("htfl3r"=>[1,2,3],"hthl3r"=>[1,2,3],"httl3r"=>[1,2,3],"htsl3r"=>[1,2,3],
		"htol3r"=>[1,2,3],"htfl2r"=>[1,2],"hthl2r"=>[1,2],"httl2r"=>[1,2],"htsl2r"=>[1,2],
		"htol2r"=>[1,2],"htf2l2r"=>[2,3],"hth2l2r"=>[2,3],"htt2l2r"=>[2,3],"hts2l2r"=>[2,3],
		"hto2l2r"=>[2,3])
	inds[stat]
end

# Return the trend for requested stat
function stat_trend!(lines::DataFrame, inds::Dict, horses::DataFrame)
	temp = DataFrame(Post=Int64[], Hnum=String[], Calc=Float64[])
	stat = horses[1, :Stat]

	for ii in 1:nrow(horses)
		l_inds = get(inds, horses[ii, :Hnum], 0)==0 ? [] : inds[horses[ii, :Hnum]]
		if length(l_inds)<2 continue end
		t_inds = trend_indexes(stat)
		if valid_trend(lines, l_inds, t_inds)
			if length(t_inds) == 2
				y1 = lines[l_inds[t_inds[2]],:Calc]
				y2 = lines[l_inds[t_inds[1]],:Calc]
				trend = 2y2 - y1
			else
				y1 = lines[l_inds[t_inds[3]],:Calc]
				y2 = (lines[l_inds[t_inds[2]],:Calc]+lines[l_inds[t_inds[1]],:Calc])/2
				trend = 2y2 - y1
			end
			push!(temp, (horses[ii, :Post], horses[ii, :Hnum], trend))
		end
	end
	temp
end

# Build and return the raw trend
@everywhere function get_trend!(data::Dict, horses::DataFrame, stat::String, lines::DataFrame, inds::Dict)
	horses[:Stat] = stat
	calculate_stat!(data, lines, inds, horses, stat)
end

# Converts stats to the approriate column in various tables
@everywhere function Factor_to_Column(stat::Char)
	symbol = Dict('F'=>"Fpos",'T'=>"Ft",'H'=>"Ht",'S'=>"Qt",'O'=>"Pays")
	symbol[stat]
end

# True if 1st in the specified race and races since
@everywhere function valid_1st(lines::DataFrame, race::Int64)

	for ii in 1:nrow(lines)
		if lines[ii, :Calc]!=1 && ii<race return false end
		if lines[ii, :Calc]==1 && ii==race return true end
	end
	false
end

# Return the requested stat 1sts found
@everywhere function stat_1st!(lines::DataFrame, inds::Dict, horses::DataFrame)
	temp = DataFrame(Post=Int64[], Hnum=String[], Calc=Float64[])
	CR = horses[1, :CR]

	for ii in 1:nrow(horses)
		l_inds = get(inds, horses[ii, :Hnum], 0)==0 ? [] : inds[horses[ii, :Hnum]]
		if valid_1st(lines[l_inds, :], CR)
			push!(temp, (horses[ii,:Post], horses[ii,:Hnum], horses[ii,:Calc]))
		end
	end
	temp
end

# return averages for requested stat
@everywhere function stat_ave!(lines::DataFrame, inds::Dict, horses::DataFrame)
	temp = DataFrame(Post=Int64[], Hnum=String[], Calc=Float64[])
	maxstarts = 0
	for a in keys(inds) maxstarts = length(inds[a])>maxstarts ? length(inds[a]) : maxstarts end
	avecum = mean(lines[:Calc])

	for ii in 1:nrow(horses)
		l_inds = get(inds, horses[ii, :Hnum], 0)==0 ? [] : inds[horses[ii, :Hnum]]
		if length(l_inds) > 0
			ave = mean(lines[:Calc][l_inds])
			starts = length(l_inds)
			ave = (ave * starts + avecum * (maxstarts - starts)) / maxstarts
			push!(temp, (horses[ii, :Post], horses[ii, :Hnum], ave))
		end
	end
	temp
end

# return minimum values for requested stat
@everywhere function stat_min!(lines::DataFrame, inds::Dict, horses::DataFrame)
	temp = DataFrame(Post=Int64[], Hnum=String[], Calc=Float64[])

	for ii in 1:nrow(horses)
		l_inds = get(inds, horses[ii, :Hnum], 0)==0 ? [] : inds[horses[ii, :Hnum]]
		if length(l_inds)>0
			push!(temp, (horses[ii, :Post], horses[ii, :Hnum], minimum(lines[l_inds, :Calc])))
		end
	end
	temp
end

# Returns true if driver, trainer, post tables needed
@everywhere function table_needed(stat::String)
    stat[1:2] in ["pc", "tc", "dc", "rc"] ? true : false
end

# A wrapper to calculate statistics such a qtime. htime, time
@everywhere function calculate_stat!(data::Dict, lines::DataFrame, inds::Dict, horses::DataFrame, stat::String)
	temp = DataFrame(Post=Int64[], Hnum=String[], Calc=Float64[])
	if nrow(lines)==0 return temp end
	functuple = load_stat_fn(stat)
	func! = functuple[1]
	lines[:Calc] = table_needed(stat) ? func!(lines, data) : func!(lines)
	aggfunc = functuple[2]
	aggfunc(lines, inds, horses)
end

@everywhere function effort(pos::Int64, out::String)
	effort = 0
	if pos == 1 effort = .1 end
	if out=="<" effort = .2
	elseif out=="<<" effort = .4
	elseif out=="<<<" effort = .6
	end
	effort
end

# More corners, mud, or first, means more effort
@everywhere function get_track_effort(ts,pos,out,var)
	out = convert(Array{String}, out)
	map((x,y,z,a)->round.((x<=3 ? 2effort(x,y)/z-a/4 : effort(x,y)/z-a/4),4), pos, out, ts, var)
end

# Calculates the magnitude of the change
@everywhere function delta!(lines::DataFrame, horses::DataFrame)

	for ii in 1:nrow(lines)
		index = findfirst(horses[:Hnum], lines[ii, :Hnum])
		lines[ii, :Calc] = horses[index, :Calc] - lines[ii, :Calc]
	end
end

# Add 0 for missing or invalid data
@everywhere function add_back_zeros(lines::DataFrame, horses::DataFrame)
	temp = zeros(nrow(horses))

	for ii in 1:nrow(horses), jj in 1:nrow(lines)
		if lines[:Hnum][jj] == horses[:Hnum][ii] temp[ii] = lines[:Calc][jj] end
	end
	temp
end

#################################################################
# All the functions to create factors

@everywhere function Driver(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	drivers = data["drivers"]
	drivers = convert(Array{Float64}, drivers[Symbol(stat)][horses[:Index]])
	adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=stats_to_STD(drivers))
end

@everywhere function Trainer(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	trainers = data["trainers"]
	trainers = convert(Array{Float64}, trainers[Symbol(stat)][horses[:Index]])
	adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=stats_to_STD(trainers))
end

@everywhere function Post(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	posts = data["posts"]
	posts = convert(Array{Float64}, posts[Symbol(stat)][horses[:Index]])
	adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=stats_to_STD(posts))
end

@everywhere function Horse(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	horsesStats = data["horses"]
	horsesStats = convert(Array{Float64}, horsesStats[Symbol(stat)][horses[:Index]])
	adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=stats_to_STD(horsesStats))
end

@everywhere function MLine(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	MLines = round.(1 ./ (1 + horses[Symbol(stat)]), 4)
	MLines = convert(Array{Float64,1}, MLines)
	if factor=="__ps_MLinR"
		adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=MLines)
	else
		adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=stats_to_STD(MLines))
	end
end

@everywhere function Hprob(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=round.((1 ./ (length(horses[:Hnum]))), 4))
end

@everywhere function HBTL(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = calculate_stat!(data, lines, inds, horses, "time")
	adjstats[:Calc] = -stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function HBxTL(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = get_stat!(factor, data, horses, stat, lines, inds)
	adjstats[:Calc] = -stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function HBE(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = get_stat!(factor, data, horses, lowercase(factor[8:9]), lines, inds)
	adjstats[:Calc] = -stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function HSSP(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	num = get_line_count(factor)
	temp = get_stat!("HBQAL$num", data, horses, "qa", lines, inds)
	stats = add_back_zeros(temp, horses)
	stat = convert(Array{Float64,1}, copy(horses[:M_Line]))
	mlineO = stat_to_O(stat)
	stats = -(stats + mlineO + horses[:Post])
	adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=stats_to_STD(stats))
end

@everywhere function HSP(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	num = get_line_count(factor)
	temp = get_stat!("HBQAL$num", data, horses, "qa", lines, inds)
	stats = add_back_zeros(temp, horses)
	stats = -(stats + horses[:Post])
	adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=stats_to_STD(stats))
end

@everywhere function BiasT(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	num = get_line_count(factor)
	temp = get_stat!("Dumb$num", data, horses, "qtime", lines, inds)
	temp[:Calc] = -stats_to_STD(temp[:Calc])
    statsQ = add_back_zeros(temp, horses)
	temp = get_stat!("Dumb$num", data, horses, "ftime", lines, inds)
	temp[:Calc] = -stats_to_STD(temp[:Calc])
	statsT = add_back_zeros(temp, horses)
	stats = map((x,y) -> (x>y ? x : y), statsQ, statsT)
	adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=stats_to_STD(stats))
end

@everywhere function BiasE(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	num = get_line_count(factor)
	ext = factor[10] == 'E' ? "e" : "a"
	temp = get_stat!("Dumb$num", data, horses, "q"*ext, lines, inds)
	temp[:Calc] = -stats_to_STD(temp[:Calc])
    statsQ = add_back_zeros(temp, horses)
	temp = get_stat!("Dumb$num", data, horses, "f"*ext, lines, inds)
	temp[:Calc] = -stats_to_STD(temp[:Calc])
	statsT = add_back_zeros(temp, horses)
	stats = map((x,y) -> (x>y ? x : y), statsQ, statsT)
	adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=stats_to_STD(stats))
end

@everywhere function BiasS(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	num = get_line_count(factor)
	temp = get_stat!("Dumb$num", data, horses, "qa", lines, inds)
	stats = add_back_zeros(temp, horses)
	if factor[10] == 'P'
		stats = -(stats + horses[:Post])
	else
		mlineO = stat_to_O(convert(Array{Float64}, copy(horses[:M_Line])))
		stats = -(stats + mlineO + horses[:Post])
	end
	statsQ = stats_to_STD(stats)
	temp = get_stat!("Dumb$num", data, horses, "fa", lines, inds)
	temp[:Calc] = -stats_to_STD(temp[:Calc])
	statsT = add_back_zeros(temp, horses)
	stats = map((x,y)->(x>y ? x : y), statsQ, statsT)
	adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=stats_to_STD(stats))
end

@everywhere function H1st(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	trainers = data["trainers"]
	horses[:Calc] = convert(Array{Float64}, trainers[Symbol(stat)][horses[:Index]])
	adjstats = get_stat!(factor, data, horses, "h1st", lines, inds)
end

@everywhere function HFL(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = get_stat!(factor, data, horses, "hfl", lines, inds)
	adjstats[:Calc] = -stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function HmFL(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = get_stat!(factor, data, horses, "hmfl", lines, inds)
	adjstats[:Calc] = -stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function HML(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = get_stat!(factor, data, horses, "hml", lines, inds)
	adjstats[:Calc] = stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function HmML(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = get_stat!(factor, data, horses, "hmml", lines, inds)
	adjstats[:Calc] = stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function HOPS(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = get_stat!(factor, data, horses, "hops", lines, inds)
	adjstats[:Calc] = -stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function HmOPS(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = get_stat!(factor, data, horses, "hmops", lines, inds)
	adjstats[:Calc] = -stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function HCL(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = get_stat!(factor, data, horses, "hcl", lines, inds)
	adjstats[:Calc] = stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function HMCL(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = get_stat!(factor, data, horses, "hMcl", lines, inds)
	adjstats[:Calc] = -stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function HmCL(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	adjstats = get_stat!(factor, data, horses, "hmcl", lines, inds)
	adjstats[:Calc] = stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function HT(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	fact = lowercase(factor[6:end])
	adjstats = get_trend!(data, horses, fact, lines, inds)
	horses[:Stat] = Factor_to_Column(factor[8])
	adjstats[:Calc] = -history_to_STD(lines, adjstats[:Calc], horses)
	adjstats
end

@everywhere function HTBx(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	ext = lowercase(factor[9])
	temp = get_trend!(data, horses, "ht"*ext*"l2r", lines, inds)
	temp[:Calc] = -stats_to_STD(temp[:Calc])
	statsl2 = add_back_zeros(temp, horses)
	temp = get_trend!(data, horses, "ht"*ext*"l3r", lines, inds)
	temp[:Calc] = -stats_to_STD(temp[:Calc])
	statsl3 = add_back_zeros(temp, horses)
	temp = get_trend!(data, horses, "ht"*ext*"2l2r", lines, inds)
	temp[:Calc] = -stats_to_STD(temp[:Calc])
	stats2l2 = add_back_zeros(temp, horses)
	best = best_trend(statsl2, statsl3, stats2l2)
	adjstats = DataFrame(Post=horses[:Post], Hnum=horses[:Hnum], Calc=stats_to_STD(best))
end

@everywhere function PC(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	horses[:Stat] = stat
	posts = data["posts"]
	horses[:Calc] = convert(Array{Float64}, posts[Symbol(stat)][horses[:Index]])
	adjstats = get_stat!(factor, data, horses, lowercase(factor[6:10]), lines, inds)
	delta!(adjstats, horses)
	adjstats[:Calc] = stats_to_STD(adjstats[:Calc])
	adjstats
end

@everywhere function TC(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	horses[:Stat] = stat
	trainers = data["trainers"]
	horses[:Calc] = convert(Array{Float64}, trainers[Symbol(stat)][horses[:Index]])
	adjstats = get_change!(factor, data, horses, lowercase(factor[6:10]), lines, inds)
	delta!(adjstats, horses)
	adjstats[:Calc] = change_to_STD(trainers, adjstats[:Calc], horses)
	adjstats
end

@everywhere function DC(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	horses[:Stat] = stat
	drivers = data["drivers"]
	horses[:Calc] = convert(Array{Float64}, drivers[Symbol(stat)][horses[:Index]])
	adjstats = get_change!(factor, data, horses, lowercase(factor[6:10]), lines, inds)
	delta!(adjstats, horses)
	adjstats[:Calc] = change_to_STD(drivers, adjstats[:Calc], horses)
	adjstats
end

@everywhere function RC(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	horses[:Stat] = stat
	posts = data["posts"]
	horses[:Calc] = convert(Array{Float64}, posts[Symbol(stat)][horses[:Index]])
	adjstats = get_change!(factor, data, horses, lowercase(factor[6:10]), lines, inds)
	delta!(adjstats, horses)
	adjstats[:Calc] = change_to_STD(posts, adjstats[:Calc], horses)
	adjstats
end

@everywhere function HS(factor::String, stat::String, data::Dict, horses::DataFrame, lines::DataFrame, inds::Dict)
	horses[:Stat] = stat
	trainers = data["trainers"]
	horses[:Calc] = convert(Array{Float64}, trainers[Symbol(stat)][horses[:Index]])
	adjstats = get_change!(factor, data, horses, lowercase(factor[6:10]), lines, inds)
	adjstats[:Calc] = history_to_STD(trainers[horses[:Index], :], adjstats[:Calc], horses)
	adjstats
end

#################################################################
# A series of functions to produce raceline stats
@everywhere function dummy(lines::DataFrame)
	lines[:Calc]
end

@everywhere function dcs!(lines::DataFrame, data::Dict)
	convert(Array{Float64}, data["drivers"][Symbol(lines[:Stat][1])][lines[:Index]])
end

@everywhere function tcs!(lines::DataFrame, data::Dict)
	convert(Array{Float64}, data["trainers"][Symbol(lines[:Stat][1])][lines[:Index]])
end

@everywhere function pcs!(lines::DataFrame, data::Dict)
	convert(Array{Float64}, data["posts"][Symbol(lines[:Stat][1])][lines[:Index]])
end

@everywhere function hmcls!(lines::DataFrame)
	-lines[:Purse]
end

@everywhere function hcls!(lines::DataFrame)
	lines[:Purse]
end

@everywhere function hopss!(lines::DataFrame)
	lines[:Pays]
end

@everywhere function hmls!(lines::DataFrame)
	lines[:Earns]
end

@everywhere function hfls!(lines::DataFrame)
	lines[:Fpos]
end

@everywhere function se!(lines::DataFrame)
 	lines[:Fpos] - lines[:Spos]
end

@everywhere function fe!(lines::DataFrame)
  	lines[:Fpos] - lines[:Tpos]
end

@everywhere function he!(lines::DataFrame)
  	lines[:Fpos] - lines[:Hpos]
end

@everywhere function qe!(lines::DataFrame)
  	lines[:Qpos] - lines[:Post]
end

@everywhere function hers!(lines::DataFrame)
 	map((x,y,z,a)->(-x-y-z-a/2), lines[:e1], lines[:e2], lines[:e3], lines[:e4])
end

@everywhere function ttimes!(lines::DataFrame)
	lines[:Calc] = map((x,y,z)->(x/z-7*log(z)-(y-120)/2),lines[:Ft],lines[:TR],lines[:Distance])
  	map((x,y,z,a,b)->(x-y-z-a-b/2), lines[:Calc],lines[:e1],lines[:e2],lines[:e3],lines[:e4])
end

@everywhere function htimes!(lines::DataFrame)
	lines[:Calc] = map((x,y,z,a)->((a-x)/z-7/2*log(z)-(y-120)/2),lines[:Ht],lines[:TR],lines[:Distance],lines[:Ft])
  	map((x,y,z,a)->(x-y-z-a/2), lines[:Calc],lines[:e2],lines[:e3],lines[:e4])
end

@everywhere function ftimes!(lines::DataFrame)
	lines[:Calc] = map((x,y,z,a)->((a-x)/z-7/4*log(z)-(y-120)/4),lines[:Tt],lines[:TR],lines[:Distance],lines[:Ft])
	map((x,y,z)->(x-y-z/2), lines[:Calc],lines[:e3],lines[:e4])
end

@everywhere function qtimes!(lines::DataFrame)
	map((x,y,z)->(x-(y-120)/4)-z,lines[:Qt],lines[:TR],lines[:e1])
end

@everywhere function times!(lines::DataFrame)
	map((x,y)->x/y-7*log(y),lines[:Ft],lines[:Distance])
end
