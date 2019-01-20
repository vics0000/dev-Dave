# using Distributed # 0.7 code
# utility_fns.jl

using DataFrames
using JLD, HDF5, CSV

@everywhere function init_race_lines()
    DataFrame(Rnum=Int64[],Date=Int64[],Track=String[],Race=Int64[],Gait=String[],Type=String[],Baby=Int64[],
        Starters=Int64[],Temp=Int64[],Purse=Int64[],Cond=String[],Itv=Int64[],Dtv=Int64[],VC=Float64[],
        Distance=Float64[],Qtime=Float64[],Htime=Float64[],Ttime=Float64[],Rtime=Float64[],
        F1=Int64[],F2=Int64[],Expay=Float64[],F12=Int64[],F22=Int64[],Ex2pay=Float64[],Expool=Int64[],
        F3=Int64[],Tripay=Float64[],Tripool=Int64[],F32=Int64[],Tri2pay=Float64[],
        F4=Int64[],Suppay=Float64[],Suppool=Int64[],F42=Int64[],Sup2pay=Float64[],
        F5=Int64[],Hipay=Float64[],Hipool=Int64[],F52=Int64[],Hi2pay=Float64[],
        DDpay=Float64[],DDpool=Int64[],DD2pay=Float64[],P3pay=Float64[],P3pool=Int64[],P32pay=Float64[],
        P4pay=Float64[],P4pool=Int64[],P42pay=Float64[],P5pay=Float64[],P5pool=Int64[],P52pay=Float64[],
        P6pay=Float64[],P6pool=Int64[],P62pay=Float64[],P8pay=Float64[],P8pool=Int64[],P82pay=Float64[],
        Winpool=Float64[],Placepool=Float64[],Showpool=Float64[],WPSpool=Float64[],
        AllROI=Float64[],Classrtg=Int64[],Todayscr=Int64[],PT=String[],PP=String[],R=String[])
end

@everywhere function init_lines()
    DataFrame(Rnum=Int64[],Hnum=String[],Head=String[],Post=Int64[],
        Earns=Float64[],Pays=Float64[],Fav=String[],Couple=String[],Dnum=String[],Tnum=String[],
        Med=String[],Equip=String[],Claim=String[],C_Price=Float64[],M_Line=Float64[],Lr=String[],Lpp=String[],
        PPn=String[],APn=String[],
        PQn=String[],Qpos=Int64[],AQn=String[],Qpark=String[],Qlen=Float64[],Qt=Float64[],
        PHn=String[],Hpos=Int64[],AHn=String[],Hpark=String[],Hlen=Float64[],Ht=Float64[],
        PTn=String[],Tpos=Int64[],ATn=String[],Tpark=String[],Tlen=Float64[],Tt=Float64[],
        PSn=String[],Spos=Int64[],ASn=String[],Spark=String[],Slen=Float64[],
        PFn=String[],FposU=Int64[],Fpos=Int64[],Flen=Float64[],Ft=Float64[],FFt=Float64[],
        Wpay=Float64[],Ppay=Float64[],Spay=Float64[],
        PaceFF=Float64[],PaceSF=Float64[],PaceTF=Float64[],PaceLF=Float64[],FSpeed=Float64[])
end

@everywhere function init_aves()
    DataFrame(Tattoo=String[],Starts=Int64[],Firsts=Float64[],Seconds=Float64[],Thirds=Float64[],
		ROI=Float64[],ROI2=Float64[],ROI3=Float64[],Val=Float64[],Val2=Float64[],Val3=Float64[],
		Pay=Float64[],PayW=Float64[],PayP=Float64[],PayS=Float64[],Ave=Float64[],MPS=Float64[])
end

@everywhere function init_posts()
	DataFrame(Track=String[],Post=Int64[],Gait=String[],Starts=Int64[],
		Firsts=Float64[],Seconds=Float64[],Thirds=Float64[],ROI=Float64[],ROI2=Float64[],ROI3=Float64[],
		Val=Float64[],Val2=Float64[],Val3=Float64[],Pay=Float64[],PayW=Float64[],PayP=Float64[],PayS=Float64[],
		Ave=Float64[],MPS=Float64[])
end

@everywhere function init_ratings()
	DataFrame(Track=String[], Starts=Int64[], Rating=Float64[])
end

@everywhere function init_tattoo()
	DataFrame(date=Int64[],track=String[],race=Int64[],Rnum=Int64[],
			Gait=String[],Post=Int64[],Tnum=String[],Dnum=String[])
end

@everywhere function parse_entry(head::String)
    v1 = tryparse(Int64,head)
    if (!isnull(v1)) return get(v1) end
    v2 = tryparse(Int64,head[1:length(head)-1])
    if (!isnull(v2)) return get(v2) end
    println("Unparseable :Head field of ", head)
    head
end

@everywhere function adjust_VC!(races::DataFrame)
	vcl5 = find(races[:VC] .< -5.0)
	vcg5 = find(races[:VC] .> 5.0)
	ft = find(races[:Cond] .== "FT")
	nft = find(races[:Cond] .!= "FT")
	inds = fast_intersect(vcl5, nft)
	races[vcl5, :VC] = -5.0
	races[inds, :VC] = 0.0
	inds = fast_intersect(vcg5, ft)
	races[vcg5, :VC] = 5.0
	races[inds, :VC] = 0.0
end

@everywhere function edit_history!(races::DataFrame)
	PP_inds = find(races[:PP] .=="T")
	ML0_inds = find(races[:M_Line] .==0.0)
	inds = fast_intersect(PP_inds, ML0_inds)
	races[inds, :M_Line] = 5
end

@everywhere function get_race_day_data(date::Int64)
	#Database Layer
	#following line can be cut
	dir = ENV["DATADIR"]
	dir = ENV["DATADIR"] * "dailies/$date/"
	println("Getting data for $date")
	horses = CSV.read(dir * "Horses.csv")
	drivers = CSV.read(dir * "Drivers.csv")
	trainers = CSV.read(dir * "Trainers.csv")
	posts = CSV.read(dir * "Posts.csv")
	tracks = CSV.read(dir * "Tracks.csv")

	#Logic layer-- manipulating data
	h_inds = index_races_by_col(horses, :Tattoo)
	d_inds = index_races_by_col(drivers, :Tattoo)
	t_inds = index_races_by_col(trainers, :Tattoo)
	track_inds = index_races_by_col(tracks, :Track)
	p_inds = index_Posts(posts)
	indexes = Dict("horses"=>h_inds,"drivers"=>d_inds,"trainers"=>t_inds,"posts"=>p_inds,"tracks"=>track_inds)

	#Why a dict and not an object?
	Dict("horses"=>horses,"drivers"=>drivers,"trainers"=>trainers,"posts"=>posts,"tracks"=>tracks,"indexes"=>indexes)
end

@everywhere function load_stats_tables()
    dir = ENV["DATADIR"]
	stats = !isfile(dir*"racestats.jld") ? StatsTables() : load(dir*"racestats.jld","stats")
end

@everywhere function save_stats_tables(stats::StatsTables)
    dir = ENV["DATADIR"]
	save(dir*"racestats.jld","stats", stats)
end

@everywhere function init_BVC_races()
    dir = ENV["DATADIR"]
    if !isfile(dir*"races_BVC.csv")
        races = init_race_lines()
        CSV.write(dir*"races_BVC.csv", races)
    end
end

@everywhere function build_cards(races::DataFrame, dps::Array{Int64}, day_dps::Array{Int64}, cnt::Int64)
	history = races[dps, :]
	edit_history!(history)
	cards = copy(races[day_dps, :])
	horses = unique(cards[:Hnum])

    for horse in horses
		index = 0
		for ii in 1:cnt
			index = findnext(history[:Hnum], horse, index+1)
			if index==0 break end
			append!(cards, history[index, :])
		end
	end
	cards
end

# Get saved racelines file or a new one.
# If doesn't exist, init blank races and lines & save to disk.
@everywhere function load_racelines()
    dir = ENV["DATADIR"]
    if !isfile(dir*"racelines.csv")
        races = init_race_lines()
        lines = init_lines()
        CSV.write(dir*"racelines.csv", races)
        CSV.write(dir*"lines.csv", lines)
    else
        races = CSV.read(dir*"racelines.csv")
    end
    races
end

@everywhere function sort_save_racelines()
    dir = ENV["DATADIR"]
	println("Sorting races")
	races = CSV.read(dir*"racelines.csv")
	sort!(races, [:Rnum])
	CSV.write(dir*"racelines.csv", races)
	println("Sorting lines")
	lines = CSV.read(dir*"lines.csv"; types=Dict("Head"=>String))
	sort!(lines, [:Rnum])
	CSV.write(dir*"lines.csv", lines)
end

@everywhere function load_all_post_lines()
#	sort_save_racelines()
    println("Loading racelines")
    dir = ENV["DATADIR"]
    races = CSV.read(dir*"racelines.csv"; types=Dict("VC"=>Float64))
    lines = CSV.read(dir*"lines.csv"; types=Dict("Head"=>String))
	lines = lines[find(lines[:Post] .> 0), :]
    println("Joining racelines")
    racelines = join(races, lines, on=[:Rnum])
	racelines, races
end

@everywhere function get_date_dps(dates, ed::Int64, sd::Int64=0, include=false)
	indexes = []

	for date in keys(dates)
		if date>=sd && date<=ed
			if date==ed && include==false break end
			indexes = [collect(dates[date][1]:dates[date][2]); indexes]
		end
	end
	indexes = convert(Array{Int64}, indexes)
end

@everywhere function index_Posts(df::DataFrame)
	indexes = Dict{String,Dict{String,Dict{Int64,Int64}}}()

	for ii in 1:nrow(df)
		if get(indexes, df[ii, :Track], 0)==0
			indexes[df[ii, :Track]] = Dict{String,Dict{Int64,Int64}}()
			indexes[df[ii, :Track]][df[ii, :Gait]] = Dict{Int64, Int64}()
			indexes[df[ii, :Track]][df[ii, :Gait]][df[ii, :Post]] = ii
		elseif get(indexes[df[ii, :Track]], df[ii, :Gait], 0)==0
			indexes[df[ii, :Track]][df[ii, :Gait]] = Dict{Int64, Int64}()
			indexes[df[ii, :Track]][df[ii, :Gait]][df[ii, :Post]] = ii
		elseif get(indexes[df[ii, :Track]][df[ii, :Gait]], df[ii, :Post], 0)==0
			indexes[df[ii, :Track]][df[ii, :Gait]][df[ii, :Post]] = ii
		end
	end
	indexes
end

@everywhere function index_races_by_post(df::DataFrame, col::Symbol)
	indexes = Dict{Int64,Array{Int64}}()

	for ii in 1:nrow(df)
		get(indexes, df[ii, col], 0)==0 ? indexes[df[ii, col]] = [ii] : push!(indexes[df[ii, col]], ii )
	end
	indexes
end

@everywhere function index_races_by_col(df::DataFrame, col::Symbol)
	indexes = Dict{String,Array{Int64}}()

	for ii in 1:nrow(df)
		get(indexes, df[ii, col], 0)==0 ? indexes[df[ii, col]] = [ii] : push!(indexes[df[ii, col]], ii )
	end
	indexes
end

@everywhere function index_races_by_date(df::DataFrame)
	indexes = Dict{Int64,Dict{Int64,Int64}}()
	date = 0

	for ii in 1:nrow(df)
		if df[ii, :Date]!=date
			date = df[ii, :Date]
			indexes[date] = Dict{Int64,Int64}()
			indexes[date][1] = ii
		end
		indexes[date][2] = ii
	end
	indexes = sort(indexes, by=x->x[1])
end

@everywhere function index_races(df::DataFrame)
	println("Building indexes")
	dates = index_races_by_date(df)
	horses = index_races_by_col(df, :Hnum)
	drivers = index_races_by_col(df, :Dnum)
	trainers = index_races_by_col(df, :Tnum)
	posts = index_races_by_post(df, :Post)
	tracks = index_races_by_col(df, :Track)
	gaits = index_races_by_col(df, :Gait)
	entries = index_races_by_col(df, :PP)
	Dict("dates" => dates, "horses" => horses, "drivers" => drivers, "trainers" => trainers,
	     "posts" => posts, "tracks" => tracks, "gaits" => gaits, "entries" => entries)
end

@everywhere function get_track_rating(ratings::DataFrame, indexes::Dict, track::String)
	index = get(indexes, track, 0)
	rate = index==0 ? ratings[1, :Rating] : ratings[index, :Rating]
	rate[1]
end

@everywhere function get_track_size(track, t_sizes::DataFrame)
	size = convert(String, t_sizes[findfirst(t_sizes[:code], track), :size])
	size = split(size, " ")
    size = length(size)==1 ? eval(parse(size[1])) : eval(parse(size[1])) + eval(parse(size[2]))
end

@everywhere function find_extreme_vals(df,predictor_symbols,val)
    predictor_symbols[find(map(x->maximum(abs.(df[x])) .>
    val,predictor_symbols))]
end

# julia's intersect function on arrays is ridiculously slow
# Key (not sure why) is Set vs array.
@everywhere function fast_intersect(a1::Array{Int64}, a2::Array{Int64})
	retval = Int64[]
	a2_set = Set(a2)

	for e in a1
		if e in a2_set push!(retval,e) end
	end
	retval
end

@everywhere function get_fraction(stat::Symbol)
	stats = Dict(:Qtime=>"qtime",:Htime=>"htime",:Ftime=>"ftime",:Ttime=>"ttime",:Her=>"her")
	stats[stat]
end

@everywhere function tdh_indexes!(indexes::Dict, horses::DataFrame, col::Symbol)

	for ii in 1:nrow(horses)
		num = horses[ii, col]
		horses[ii, :Index] = num=="-" ? 1 : indexes[num][1]
	end
end

@everywhere function post_indexes!(indexes::Dict, horses::DataFrame)

	for ii in 1:nrow(horses)
		post = horses[ii, :Post]>20 ? 1 : horses[ii, :Post]
		horses[ii, :Index] = indexes[horses[ii, :Track]][horses[ii, :Gait]][post][1]
	end
end

@everywhere function get_startdate(ed::Int64, months=12)
	df = Dates.DateFormat("yyyymmdd")
	parse(Int64, (Dates.format(((Date("$ed",df)) - Dates.Month(months)), df)))
end

@everywhere function get_tattoo_history(date::Int64, tattoo::String)
	dir = ENV["DATADIR"] * "tattoos/"
	if !isfile(dir*"$tattoo.csv") return init_tattoo() end
	races = CSV.read(dir*"$tattoo.csv"; types=Dict("Tnum"=>String, "Dnum"=>String))
	races = races[find(races[:date] .<= date), :]
	races = races[find(races[:Rnum] .> 0), :]
end

@everywhere function update_tattoo!(hash)
    date = hash[1]
    track = "$(hash[2])"
    race = hash[3]
    tattoo = "$(hash[4])"
	rnum = length(hash)==5 ? hash[5] : 0
	if length(hash)==9
		rnum = hash[5]
		gait = "$(hash[6])"
		post = hash[7]
		tnum = "$(hash[8])"
		dnum = "$(hash[9])"
	else
		gait = "-"
		post = 0
		tnum = "-"
		dnum = "-"
	end
    dir = ENV["DATADIR"] * "tattoos/"
    if !isdir(dir) mkpath(dir) end
	races = init_tattoo()
	if isfile(dir*"$tattoo.csv")
		f = open(dir*"$tattoo.csv")
        header = readline(f)
        close(f)
		if length(split(header, ","))<8
			races = CSV.read(dir*"$tattoo.csv")
		else
			races = CSV.read(dir*"$tattoo.csv";types=Dict("Tnum"=>String,"Dnum"=>String))
		end
	end
	if length(names(races))<8
		races[:Rnum] = 0
		races[:Gait] = "-"
		races[:Post] = 0
		races[:Tnum] = "-"
		races[:Dnum] = "-"
	end
    detail = date, track, race, rnum, gait, post, tnum, dnum
	index = findfirst(races[:date], date)
	index!=0 ? for ii in 1 : length(detail) races[index, ii] = detail[ii] end : push!(races, detail)
	sort!(races, rev=true)
    CSV.write(dir*"$tattoo.csv", races)
end

@everywhere function update_lines(hash, data::Tables)
    dir = ENV["DATADIR"]
    date = hash[1]
    track = hash[2]
    racenum = hash[3]
    race = data.lines_by_date[date].tracks[track].races[racenum]
    lines = Dict{String, Any}()
    for tattoo in keys(race.lines)
        lines[tattoo] = (tattoo,"0",0,0.0,0.0,"","",0,0,"","","",0.0,"-","F","F")
    end
    dir = ENV["DATADIR"] * "dailies/$date/$track/$racenum/"
    fname = dir * "lines.csv"
    if isfile(fname)
        f = open(fname)
        header = readline(f)
        for l in eachline(f)
            line = split(l, ",")
            lines[line[1]] = line
        end
        close(f)
    end
    f = open(dir*"lines.csv", "w")
    write(f,"tattoo,head,post,earns,pays,fav,coupled,dnum,tnum,med,equip,claim,c_price,m_line,r,pp\n")

    for tattoo in keys(race.lines)
        h = race.lines[tattoo]
        save_points(dir*"tattoos/$tattoo/", h.point_of_calls)
        save_finish(dir*"tattoos/$tattoo/", h.finish)
        save_speed(dir*"tattoos/$tattoo/", h.speed_figures)
        save_pays(dir*"tattoos/$tattoo/", h.payouts)
        l = lines[tattoo]
        head = l[2]!="0" ? l[2] : h.program_number
        med = l[10]!="" ? l[10] : h.medication
        equip = l[11]!="" ? l[11] : h.equipment
        c_price = l[13]!=0 ? l[13] : h.claimprice
        detail = tattoo,head,h.post,h.earnings,h.oddstoadollar,h.favorite,h.coupled,
            h.dnum,h.tnum,med,equip,h.claim_indicator,c_price,l[14],"T",l[16]
        write(f, join(detail,","), "\n")
    end
    close(f)
end

@everywhere function update_lines_pp(hash, data::Tables)
    dir = ENV["DATADIR"]
    date = hash[1]
    track = hash[2]
    racenum = hash[3]
    race = data.lines_by_date[date].tracks[track].races[racenum]
    lines = Dict{String, Any}()
    dir = ENV["DATADIR"] * "dailies/$date/$track/$racenum/"
    fname = dir * "lines.csv"
    if isfile(fname)
        f = open(fname)
        header = readline(f)
        for l in eachline(f)
            line = split(l, ",")
            lines[line[1]] = line
        end
        close(f)
    else
        for tattoo in keys(race.lines)
            lines[tattoo] = ("","0",0,0.0,0.0,"","",0,0,"","","",0.0,"-","F","F")
        end
    end
    f = open(dir*"lines.csv", "w")
    write(f,"tattoo,head,post,earns,pays,fav,coupled,dnum,tnum,med,equip,claim,c_price,m_line,r,pp\n")

    for tattoo in keys(race.lines)
        h = race.lines[tattoo]
        l = lines[tattoo]
        head = l[2]!="0" ? l[2] : h.program_number
        post = l[3]!=0 ? l[3] : h.post
        dnum = l[8]!=0 ? l[8] : h.dnum
        tnum = l[9]!=0 ? l[9] : h.tnum
        med = l[10]!="" ? l[10] : h.medication
        equip = l[11]!="" ? l[11] : h.equipment
        c_price = l[13]!=0 ? l[13] : h.claimprice
        detail = tattoo,head,post,l[4],l[5],l[6],l[7],dnum,tnum,med,equip,l[12],c_price,h.m_line,l[15],"T"
        write(f, join(detail,","), "\n")
    end
    close(f)
end

@everywhere function update_race(hash, r::Race)
    date = hash[1]
    track = hash[2]
    racenum = hash[3]
    dir = ENV["DATADIR"] * "dailies/$date/$track/$racenum/"
    if !isdir(dir) mkpath(dir) end
    fname = dir * "race.csv"
    if isfile(fname)
        open(fname) do f
            race = split(read(f, String), ",")
        end
    else race = raceDetails() end
    detail = race[1],r.race_type,r.purse,r.race_gait,r.distance,r.trk_cond,r.temperature,r.field_size,
        r.itv,r.dtv,race[11],race[12],r.classrtg,r.off_time,"T",strip(race[16])
    write(dir*"race.csv", join(detail,","))
    times = []

    for ii in 1:length(r.race_times) times = [times; r.race_times[ii].time] end
    write(dir*"times.csv", join(times,","), "\n")
    wps = r.wagers.wps
    detail = wps.win_pool,wps.place_pool,wps.show_pool,wps.total_pool
    write(dir*"wps.csv", join(detail,","), "\n")
    f = open(dir*"exotics.csv", "w")
    write(f,"type_key:wager:result:payoff:carryover:base:pool:cancel:npaid:refund\n")
    e = r.wagers.exotics

    for ii in 1:length(e)
        detail = e[ii].wager.type_key,e[ii].wager.wager,e[ii].result_string,e[ii].payoffs,
            e[ii].carryover,e[ii].base_wager,e[ii].pool_total,e[ii].cancelled,e[ii].not_paid,e[ii].refunded
        write(f, join(detail,":"), "\n")
    end
    close(f)
end

@everywhere function update_race_pp(hash, r::Race)
    date = hash[1]
    track = hash[2]
    racenum = hash[3]
    dir = ENV["DATADIR"] * "dailies/$date/$track/$racenum/"
    if !isdir(dir) mkpath(dir) end
    fname = dir * "race.csv"
    if isfile(fname)
        open(fname) do f
            race = split(read(f, String), ",")
        end
    else race = raceDetails() end
    dist = parse(Float64, r.racedata["distance_m"])
    starters = race[7]!=0 ? race[7] : r.racedata["starters"]
    pt = race[14]!="" ? strip(race[14]) : r.racedata["posttime"]
    detail = race[1],race[2],r.racedata["purse"],r.racedata["gait"][1],dist,race[6],race[7],starters,
        race[9],race[10],r.racedata["all_roi"],r.racedata["todays_cr"],race[13],pt,race[15],"T"
    write(dir*"race.csv", join(detail,","), "\n")
end

@everywhere function save_tracks!(data::Tables)
    dir = ENV["DATADIR"]
    if !isdir(dir) mkpath(dir) end
    if isfile(dir*"tracks.csv")
        tracks = CSV.read(dir*"tracks.csv"; delim=':')
    else
        tracks = DataFrame(code=String[],name=String[],size=String[])
        CSV.write(dir*"tracks.csv", tracks;  delim=':')
    end
    temp = DataFrame(code=String[],name=String[],size=String[])
    found = false

    for code in keys(data.tracks), ii in 1:nrow(tracks)
        if code==tracks[ii, :code]
            detail = data.tracks[code].code, data.tracks[code].name, "$(data.tracks[code].size)"
            if data.source!="pps" && tracks[ii, :name]=="-"
                tracks[ii, :code] = code
                tracks[ii, :name] = data.tracks[code].name
                tracks[ii, :size] = "$(data.tracks[code].size)"
                println("New track $detail")
                CSV.write(dir*"tracks.csv", tracks; delim=':')
            end
            found = true
            break
        end
    end
    if !found
        for code in keys(data.tracks)
            detail = data.tracks[code].code, data.tracks[code].name, "$(data.tracks[code].size)"
            push!(temp, detail)
            println("New track $detail")
        end
        CSV.write(dir*"tracks.csv", temp;  delim=':', append=true)
    end
    data.tracks = Dict{String,Track}()
end

@everywhere function append_forms(fname::String, form::DataFrame)
	if !isfile(fname)
        header_strings = reshape(map(x->string(x), names(form)), 1, :)
       f_out = open(fname, "w")
        writecsv(f_out, header_strings)
        close(f_out)
    end
	f_out = open(fname, "a")
    A = Array{Any,2}
    try A = convert(Array{Any,2}, form)
    catch error
        term_indexes = names(form)

	    for index in term_indexes
	        cnt = length(find(ismissing.(form[index])))
	        if cnt > 0
	            println("$cnt NA found for term: ",index)
                form[find(ismissing.(form[:, index])), index] = "NA"
	        end
	    end
        A = convert(Array{Any,2}, form)
    end
    writecsv(f_out, A)
    close(f_out)
end

@everywhere function save_races!(data::Tables)
    data.new_race = "N"
    dir = ENV["DATADIR"]
    races = isfile(dir*"races.csv") ? CSV.read(dir*"races.csv") : DataFrame(date=Int64[],track=String[],race=Int64[])

    for date in keys(data.lines_by_date)
        for track in keys(data.lines_by_date[date].tracks)
            for race in keys(data.lines_by_date[date].tracks[track].races)
                detail = date, track, race
                push!(races, detail)
            end
        end
    end
    unique!(races, [:date, :track, :race])
	sort!(races, [:date, :track, :race])
    CSV.write(dir*"races.csv", races)
end

@everywhere function save_horses!(data::Tables)
    dir = ENV["DATADIR"]
    file = dir * "horses.csv"
    if isfile(dir*"horses.csv")
        horses = CSV.read(dir*"horses.csv")
    else
        horses = DataFrame(tattoo=String[],name=String[],sex=String[],sire=String[],dam=String[],damsire=String[])
        CSV.write(dir*"horses.csv", horses)
    end
    temp = DataFrame(tattoo=String[],name=String[],sex=String[],sire=String[],dam=String[],damsire=String[])

    for tattoo in keys(data.horses), ii in 1:nrow(horses)
        if tattoo==horses[ii, :tattoo]
            data.horses[tattoo].found = "T"
            continue
        end
    end

    for tattoo in keys(data.horses)
        h = data.horses[tattoo]
        if h.found=="F"
            println("New horse $(h.name)")
            detail = tattoo, h.name, h.horse_sex, h.horse_sire, h.horse_dam, h.horse_damsire
            push!(temp, detail)
        end
    end
    CSV.write(dir*"horses.csv", temp; append=true)
    data.horses = Dict{String,horseDetails}()
end

@everywhere function save_trainers!(data::Tables)
    dir = ENV["DATADIR"]
    file = dir * "trainers.csv"
    if isfile(dir*"trainers.csv")
        trainers = CSV.read(dir*"trainers.csv"; delim=':')
    else
        trainers = DataFrame(code=String[],name=String[])
        CSV.write(dir*"trainers.csv", trainers; delim=':')
    end
    temp = DataFrame(code=String[],name=String[])

    for code in keys(data.trainers)
        for ii in 1:nrow(trainers)
            if code==trainers[ii, :code]
                delete!(data.trainers, code)
                break
            end
        end
    end

    for code in keys(data.trainers)
        println("New trainer $(data.trainers[code])")
        detail = code, data.trainers[code]
        push!(temp, detail)
    end
    CSV.write(dir*"trainers.csv", temp; delim=':', append=true)
    data.trainers = Dict{String,String}()
end

@everywhere function save_drivers!(data::Tables)
    dir = ENV["DATADIR"]
    file = dir * "drivers.csv"
    if isfile(dir*"drivers.csv")
        drivers = CSV.read(dir*"drivers.csv"; delim=':')
    else
        drivers = DataFrame(code=String[],name=String[])
        CSV.write(dir*"drivers.csv", drivers; delim=':')
    end
    temp = DataFrame(code=String[],name=String[])

    for code in keys(data.drivers)
        for ii in 1:nrow(drivers)
            if code==drivers[ii, :code]
                delete!(data.drivers, code)
                break
            end
        end
    end

    for code in keys(data.drivers)
        println("New driver $(data.drivers[code])")
        detail = code, data.drivers[code]
        push!(temp, detail)
    end
    CSV.write(dir*"drivers.csv", temp; delim=':', append=true)
    data.drivers = Dict{String,String}()
end

@everywhere function save_points(dir::String, points::Array{Point})
    if !isdir(dir) mkpath(dir) end
    f = open(dir*"points.csv", "w")
    write(f,"call,before,position,after,parked,lengths,horse_time\n")

    for ii in 1:length(points)
        p = points[ii]
        detail = p.call, p.before, p.position, p.after, p.parked, p.lengths, p.horse_time
        write(f, join(detail,","), "\n")
    end
    close(f)
end

@everywhere function save_finish(dir::String, finish::Array{FinishPoc})
    f = open(dir*"finish.csv", "w")
    if length(finish)!=1
#        println("save_finish $dir ", finish)
        if length(finish)==0
            close(f)
            return
        end
        fred
    end
    r = finish[1]
    detail = r.call,r.before,r.original,r.official,r.parked,r.lengths,r.horse_time,r.final_fraction_time
    write(f, join(detail,","), "\n")
    close(f)
end

@everywhere function save_speed(dir::String, ratings::Array{SpeedPoc})
    f = open(dir*"ratings.csv", "w")
    if length(ratings)!=1
#        println("save_speed  $dir ", ratings)
        if length(ratings)==0
            close(f)
            return
        end
        fred
    end
    r = ratings[1]
    detail = r.pace_first_fraction,r.pace_second_fraction,r.pace_third_fraction,r.pace_final_fraction,
        r.final_speed
    write(f, join(detail,","), "\n")
    close(f)
end

@everywhere function save_pays(dir::String, pays::Array{Payout})
    f = open(dir*"pays.csv", "w")
    write(f,"wager,pays\n")

    for ii in 1:length(pays)
        detail = pays[ii].pos, pays[ii].amount
        write(f, join(detail,","), "\n")
    end
    close(f)
end

@everywhere function save_tables(data::Tables)
    save_tracks!(data)
    if data.new_race=="Y" save_races!(data) end
    save_horses!(data)
    save_trainers!(data)
    save_drivers!(data)
end

@everywhere function load_lines!(hash, data::Tables)
    r = "F"
    p = "F"
    date = hash[1]
    track = hash[2]
    race = hash[3]
    dir = ENV["DATADIR"] * "dailies/$date/$track/$race/"
    lines = isfile(dir*"lines.csv") ? CSV.read(dir*"lines.csv") : DataFrame()

    for ii in 1:nrow(lines)
        r = lines[1, :r]
        p = lines[1, :pp]
        tattoo = lines[ii, :tattoo]
        if get(data.lines_by_date[date].tracks[track].races[race].lines, tattoo, 0)==0
            data.lines_by_date[date].tracks[track].races[race].lines[tattoo] = Starter()
        end
    end
    r, p
end

@everywhere function load_races!(chart::Chart, data::Tables)
    date = parse(Int64, replace(chart.race_date, "-", ""))
    code = strip(chart.track.code)
    data.lines_by_date = Dict{Int64,Lines}()
    data.lines_by_date[date] = Lines()
    data.lines_by_date[date].tracks[code] = l_by_race()
    track = data.lines_by_date[date].tracks[code]

    for ii in keys(chart.trackdata.racedata)
        track.races[ii] = l_by_date()
        race = chart.trackdata.racedata[ii]
        dir = ENV["DATADIR"] * "dailies/$date/$code/$ii/"
        hash = [date, code, ii]
        pp = "F"
        if isfile(dir*"race.csv")
            open(dir*"race.csv") do f
                r = split(read(f, String), ",")
                pp = r[16]
            end
        end
        if pp=="F"
            update_race_pp(hash, race)
            data.new_race = "Y"
        end

        for jj in keys(race.starters)
            race.starters[jj].registration_num = race.starters[jj].details["regno"]
            tattoo = race.starters[jj].registration_num
            track.races[ii].lines[tattoo] = race.starters[jj]
        end
    end
end

# In the Dailies/date/track directory, get the race folders (1, 2, 3, etc),
# ignoring files like .DS_store
@everywhere function getNumRaceFolders(trackDir::AbstractString)
	numRaceFolders = 0
	for currentFile in readdir(trackDir)
		if isdir(trackDir * currentFile) && tryparse(Float64, currentFile) != nothing
			numRaceFolders += 1
		end
	end
	numRaceFolders
end

@everywhere function parse_races!(chart::Chart, data::Tables)
    date = parse(Int64, replace(chart.race_date, "-", ""))
    code = strip(chart.track.code)
    data.lines_by_date = Dict{Int64,Lines}()
    data.lines_by_date[date] = Lines()
    data.lines_by_date[date].tracks[code] = l_by_race()
    track = data.lines_by_date[date].tracks[code]
	numRaceFolders = 0

	for ii in 1:length(chart.races)
		found = false
		mkpath(ENV["DATADIR"]*"dailies/$date/$code/")
		trackDir = ENV["DATADIR"]*"dailies/$date/$code/"
		numRaceFolders = getNumRaceFolders(trackDir)
		rind = 0
		# Check each race folder for the horse.
		# numRaceFolders will grow if new races found.
		for jj in 1:numRaceFolders
			found = false
			dir = ENV["DATADIR"] * "dailies/$date/$code/$jj/"
			# if not a directory, this is an error
			if !isdir(dir)
				error("Whoa! $dir does not exist")
				break
			end
			f = open(dir * "lines.csv")
			readline(f)
			for ln in eachline(f), starter in chart.races[ii].starters
				line = split(ln, ",")
				if line[1]==starter.registration_num
					found = true
					rind = jj
					break
				end
			end
			close(f)
			if found
				break
			end
		end
		if found
			index = rind
		else
			#New horse not in any prev race, so increase number of races.
			numRaceFolders += 1
			index = numRaceFolders
			data.new_race = "Y"
			println("Adding race $index at $code")
			println("numRaceFolders = $numRaceFolders")
		end
		track.races[index] = l_by_date()
		chart.races[ii].index = index
		hash = [date, code, index]
		# Save the race data, creating new folder & files if
		# necessary. Race, Times, WPS, and Exotics.csv
		update_race(hash, chart.races[ii])

		for kk in 1:length(chart.races[ii].starters)
			horse = chart.races[ii].starters[kk]
			track.races[index].lines[horse.registration_num] = horse
		end
		process_lines!(hash, chart.races[ii], data)
	end
end
