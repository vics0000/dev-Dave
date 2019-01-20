# build_races.jl
# using Distributed # 0.7 code
@everywhere include("config.jl")

#@everywhere ENV["DATADIR"] = "/home/dave/Races/data/"

@everywhere include(ENV["TMDIR"]*"alt_type.jl")
@everywhere include(ENV["TMDIR"]*"utility_fns.jl")

# Remove extra brackets, dollar signs other artifacts
# from single row of a dataframe
@everywhere function clean_result!(result)
    # 0.7 function body
    # result[:payoff] = replace(result[:payoff], collect("[]") => "")
    # result[:wager] = replace(result[:wager], collect("\$") => "")
    # result[:result] = replace(result[:result], r"=-|=|--|//", "-")
    # result[:result] = replace(result[:result], r"All|ALLL|ALL|alll|all|a|\*|X" => "A")
    # result[:result] = replace(result[:result], r"1([A-C])" => "1")
    # result[:result] = replace(result[:result], collect("[]`+LPWY\\") => "")
    # result[:result] = replace(result[:result], " - " => " ")

    result[1, :payoff] = replace(result[1, :payoff], collect("[]"), "")
    result[1, :wager] = replace(result[1, :wager], collect("\$"), "")
    result[1, :result] = replace(result[1, :result], r"=-|=|--|//", "-")
    result[1, :result] = replace(result[1, :result], r"All|ALLL|ALL|alll|all|a|\*|X", "A")
    result[1, :result] = replace(result[1, :result], r"1([A-C])", "1")
    result[1, :result] = replace(result[1, :result], collect("[]`+LPWY\\"), "")
    result[1, :result] = replace(result[1, :result], " - ", " ")
end

# Add pay pools to details of Exotics
@everywhere function add_pay_pool!(cnt::Int64, base::Float64, e, detail)
    # pays = split(e[:payoff], ",")
    pays = split(e[1, :payoff], ",")
    payout = cnt==2 ? parse(Float64,pays[2])/base : parse(Float64,pays[1]) == 2 && length(pays) == 2 ?
        parse(Float64,pays[2])/base : parse(Float64,pays[1])/base
    payout = round.(payout, 2)
    # 0.7 code begin
    # if e[:type_key] == 104
    #     detail[7] = e[:pool]
    #     cnt == 1 ? detail[3] = payout : detail[6] = payout
    # elseif e[:type_key] == 106
    #     detail[10] = e[:pool]
    #     cnt == 1 ? detail[9] = payout : detail[12] = payout
    # elseif e[:type_key] == 107
    #     detail[15] = e[:pool]
    #     cnt == 1 ? detail[14] = payout : detail[17] = payout
    # elseif e[:type_key] == 108
    #     detail[24] = e[:pool]
    #     cnt == 1 ? detail[23] = payout : detail[25] = payout
    # elseif e[:type_key]==109
    #     detail[27] = e[:pool]
    #     cnt == 1 ? detail[26] = payout : detail[28] = payout
    # elseif e[:type_key]==110
    #     detail[30] = e[:pool]
    #     cnt==1 ? detail[29] = payout : detail[31] = payout
    # elseif e[:type_key] == 111
    #     detail[33] = e[:pool]
    #     cnt==1 ? detail[32] = payout : detail[34] = payout
    # elseif e[:type_key] == 116 || e[:type_key] == 117
    #     detail[20] = e[:pool]
    #     cnt == 1 ? detail[19] = payout : detail[22] = payout
    # elseif e[:type_key]==112
    #     detail[36] = e[:pool]
    #     cnt==1 ? detail[35] = payout : detail[37] = payout
    # elseif e[:type_key]==114
    #     detail[39] = e[:pool]
    #     cnt == 1 ? detail[38] = payout : detail[40] = payout
    # end
    # 0.7 code end

    # 0.6 code begin
    if e[1, :type_key]==104
        detail[7] = e[1, :pool]
        cnt==1?detail[3] = payout: detail[6] = payout
    elseif e[1, :type_key]==106
        detail[10] = e[1, :pool]
        cnt==1?detail[9] = payout: detail[12] = payout
    elseif e[1, :type_key]==107
        detail[15] = e[1, :pool]
        cnt==1?detail[14] = payout: detail[17] = payout
    elseif e[1, :type_key]==108
        detail[24] = e[1, :pool]
        cnt==1?detail[23] = payout: detail[25] = payout
    elseif e[1, :type_key]==109
        detail[27] = e[1, :pool]
        cnt==1?detail[26] = payout: detail[28] = payout
    elseif e[1, :type_key]==110
        detail[30] = e[1, :pool]
        cnt==1?detail[29] = payout: detail[31] = payout
    elseif e[1, :type_key]==111
        detail[33] = e[1, :pool]
        cnt==1?detail[32] = payout: detail[34] = payout
    elseif e[1,:type_key]==116||e[1, :type_key]==117
        detail[20] = e[1, :pool]
        cnt==1?detail[19] = payout: detail[22] = payout
    elseif e[1, :type_key]==112
        detail[36] = e[1, :pool]
        cnt==1?detail[35] = payout: detail[37] = payout
    elseif e[1, :type_key]==114
        detail[39] = e[1, :pool]
        cnt==1 ? detail[38] = payout: detail[40] = payout
    end
    #0.6 code end
end

# I don't know what Heads are.
@everywhere function add_heads!(cnt::Int64, heads, e, detail)
    # 0.7 code begin
    # if e[:type_key] >= 108 && e[:type_key] <= 114 return end
    # if length(heads) <= 1 || isempty(heads[1]) || isempty(heads[2]) return end
    # if e[:type_key] == 106 && (length(heads) <3 || isempty(heads[3]) || heads[3] == "A") return end
    # if e[:type_key] == 107 && (length(heads) <4 || isempty(heads[4]) || heads[4] == "A") return end
    # if e[:type_key] == 116 && (length(heads) <5 || isempty(heads[5]) || heads[5] == "A") return end
    # if e[:type_key] == 117 && (length(heads) <5 || isempty(heads[5]) || heads[5] == "A") return end
    # if heads[1]=="A" || heads[2] == "A" return end
    # if e[:type_key] == 104 || e[:type_key] == 106 || e[:type_key] == 107 || e[:type_key] == 116 || e[:type_key] == 117
    # 0.7 code end

    #0.6 code begin
    if e[1,:type_key]>=108 && e[1,:type_key]<=114 return end
    if length(heads)<=1 || isempty(heads[1]) || isempty(heads[2]) return end
    if e[1,:type_key]==106&&(length(heads)<3||isempty(heads[3])||heads[3]=="A") return end
    if e[1,:type_key]==107&&(length(heads)<4||isempty(heads[4])||heads[4]=="A") return end
    if e[1,:type_key]==116&&(length(heads)<5||isempty(heads[5])||heads[5]=="A") return end
    if e[1,:type_key]==117&&(length(heads)<5||isempty(heads[5])||heads[5]=="A") return end
    if heads[1]=="A" || heads[2]=="A" return end
    if e[1,:type_key]==104||e[1,:type_key]==106||e[1,:type_key]==107||e[1,:type_key]==116||e[1,:type_key]==117
    #0.6 code end
        if cnt == 1
            detail[1] = parse(Int64, heads[1])
            detail[2] = parse(Int64, heads[2])
        else
            detail[4] = parse(Int64, heads[1])
            detail[5] = parse(Int64, heads[2])
        end
    end
    # 0.7 code begin
    # if e[:type_key] == 106 || e[:type_key] == 107 || e[:type_key] == 116 || e[:type_key] == 117
    #     cnt == 1 ? detail[8] = parse(Int64, heads[3]) : detail[11] = parse(Int64, heads[3])
    # end
    # if e[:type_key] == 107 || e[:type_key] == 116 || e[:type_key] == 117
    #     cnt == 1 ? detail[13] = parse(Int64, heads[4]) : detail[16] = parse(Int64, heads[4])
    # end
    # if e[:type_key] == 116 || e[:type_key] == 117
    #     cnt == 1 ? detail[18] = parse(Int64, heads[5]) : detail[21] = parse(Int64, heads[5])
    # end
    # 0.7 code end

    # 0.6 code begin
    if e[1,:type_key] == 106 || e[1,:type_key] == 107 || e[1,:type_key] == 116 || e[1,:type_key] == 117
        cnt == 1 ? detail[8] = parse(Int64, heads[3]) : detail[11] = parse(Int64, heads[3])
    end
    if e[1,:type_key] == 107 || e[1,:type_key] == 116 || e[1,:type_key] == 117
        cnt == 1 ? detail[13] = parse(Int64, heads[4]) : detail[16] = parse(Int64, heads[4])
    end
    if e[1,:type_key] == 116 || e[1,:type_key] == 117
        cnt == 1 ? detail[18] = parse(Int64, heads[5]) : detail[21] = parse(Int64, heads[5])
    end
    # 0.6 code end
end

# Get results of Exotics
@everywhere function add_result!(result, detail)
    clean_result!(result)
    # 0.7 code begin
    # pays = split(result[:payoff], ",")
    # if result[:refund] == true return end
    # if result[:cancel] == true return end
    # if result[:npaid] == true return end
    # if parse(Float64, pays[1]) == 0 return end
    # exotic = split(result[:result], "),")
    # wager = split(result[:wager])
    # base = tryparse(Float64, wager[1])
    # base = base == nothing ? 2.0  : base
    # base = base > 2.0 ? base / 100 : base
    # 0.7 code end

    # 0.6 code begin
    pays = split(result[1, :payoff], ",")
    if result[1, :refund]==true return end
    if result[1, :cancel]==true return end
    if result[1, :npaid]==true return end
    if parse(Float64, pays[1])==0 return end
    exotic = split(result[1, :result], "),")
    wager = split(result[1, :wager])
    base = isnull(tryparse(Float64, wager[1]))? 2.0: parse(Float64, wager[1])
    base = base>2.0? base / 100: base
    # 0.6 code end

    oldhorses = ""

    for ii in 1:length(pays)
        if ii == 2 && (parse(Float64, pays[1]) == 2 || parse(Float64, pays[end]) == 0) break end
        rec = ii == 2 && length(exotic) == 1 ? split(exotic[1])[3] : strip(replace(exotic[ii], collect("()"), ""))
        rec = replace(rec, collect("\$"), "")
        recfields = split(rec)

        if length(recfields)>2
            # 0.6 code begin
            if !isnull(tryparse(Float64, recfields[1]))
                base = parse(Float64, recfields[1])
                horses = length(recfields)>=5? recfields[4]: recfields[2]
            else
                !isnull(tryparse(Float64,recfields[2]))? base = parse(Float64,recfields[2]):
                println("What the heck -->", recfields)
                horses = recfields[1]
            end
            # 0.6 code end

            # 0.7 code begin
            # base = tryparse(Float64, recfields[1])
            # if base != nothing
            #     horses = length(recfields) >= 5 ? recfields[4] : recfields[2]
            # else
            #     base = tryparse(Float64, recfields[2])
            #     if base == nothing
            #         println("Base == nothing. What the heck -->", recfields)
            #         # TODO: Ask Dave if ok to set base to 2.0 here as above
            #         base = 2.0
            #     end
            #     horses = recfields[1]
            # end
            # 0.7 code end

            if oldhorses == horses break end
            oldhorses = horses
            base = base > 2.0 ? base / 100 : base
            add_pay_pool!(ii, base, result, detail)
            horses = replace(horses, collect(".,;"), "")
            horses = replace(horses, r"4OF5|5OF5", "")
            heads = split(horses, r"-|/")
        else
            add_pay_pool!(ii, base, result, detail)
            horses = replace(recfields[1], collect(".,;"), "")
            horses = replace(horses, r"4OF5|5OF5", "")
            heads = split(horses, r"-|/")
        end
        add_heads!(ii, heads, result, detail)
    end
end

# Exotics come from trackmaster. Each code is a kind of exotic.
@everywhere function get_exotics(hash)
    detail = [0,0,0.0,0,0,0.0,0,  0,0.0,0,0,0.0,  0,0.0,0,0,0.0,  0,0.0,0,0,0.0,
        0.0,0,0.0,0.0,0,0.0,  0.0,0,0.0,0.0,0,0.0,  0.0,0,0.0,0.0,0,0.0]
    dir = ENV["DATADIR"] * "dailies/$(hash[1])/$(hash[2])/$(hash[3])/"
    e = CSV.read(dir*"exotics.csv"; delim=':')
    println(dir)

    for ii in 1:nrow(e)
        if e[ii, :type_key]==104 add_result!(e[ii, :], detail)
        elseif e[ii, :type_key]==106 add_result!(e[ii, :], detail)
        elseif e[ii, :type_key]==107 add_result!(e[ii, :], detail)
        elseif e[ii, :type_key]==108 add_result!(e[ii, :], detail)
        elseif e[ii, :type_key]==109 add_result!(e[ii, :], detail)
        elseif e[ii, :type_key]==110 add_result!(e[ii, :], detail)
        elseif e[ii, :type_key]==111 add_result!(e[ii, :], detail)
        elseif e[ii, :type_key]==117 add_result!(e[ii, :], detail)
        elseif e[ii, :type_key]==112 add_result!(e[ii, :], detail)
        elseif e[ii, :type_key]==116 add_result!(e[ii, :], detail)
        elseif e[ii, :type_key]==114 add_result!(e[ii, :], detail)
        elseif e[ii, :type_key]==105
        elseif e[ii, :type_key]==115
        elseif e[ii, :type_key]==118
        elseif e[ii, :type_key]==0 println(e)
        else println(e)
        end
    end
    detail
end

# Get Points from dailies and add details for Lines
@everywhere function add_points_to_details(hash, detail)
    detail = [detail; "-";"-"; "-";0;"-";"-";0.0;0.0; "-";0;"-";"-";0.0;0.0;
        "-";0;"-";"-";0.0;0.0; "-";0;"-";"-";0.0]
    dir = ENV["DATADIR"] * "dailies/$(hash[1])/$(hash[2])/$(hash[3])/tattoos/$(detail[2])/"
    if !isfile(dir*"points.csv") return detail end
    p = CSV.read(dir*"points.csv")
    if nrow(p)==0 return detail end

    for ii in 1:nrow(p)
        if p[ii, :call]=="Post"
            if p[ii, :position]!=detail[4] detail[4] = p[ii, :position] end
            if !ismissing.(p[ii, :before]) detail[18] = strip(p[ii, :before]) end
            if !ismissing.(p[ii, :after]) detail[19] = strip(p[ii, :after]) end
        elseif p[ii, :call]=="1"
            detail[21] = p[ii, :position]
            if !ismissing.(p[ii, :before]) detail[20] = strip(p[ii, :before]) end
            if !ismissing.(p[ii, :after]) detail[22] = strip(p[ii, :after]) end
            if !ismissing.(p[ii, :parked]) detail[23] = strip(p[ii, :parked]) end
            detail[24] = p[ii, :lengths]
            detail[25] = p[ii, :horse_time]
        elseif p[ii, :call]=="2"
            detail[27] = p[ii, :position]
            if !ismissing.(p[ii, :before]) detail[26] = strip(p[ii, :before]) end
            if !ismissing.(p[ii, :after]) detail[28] = strip(p[ii, :after]) end
            if !ismissing.(p[ii, :parked]) detail[29] = strip(p[ii, :parked]) end
            detail[30] = p[ii, :lengths]
            detail[31] = p[ii, :horse_time]
        elseif p[ii, :call]=="3"
            detail[33] = p[ii, :position]
            if !ismissing.(p[ii, :before]) detail[32] = strip(p[ii, :before]) end
            if !ismissing.(p[ii, :after]) detail[34] = strip(p[ii, :after]) end
            if !ismissing.(p[ii, :parked]) detail[35] = strip(p[ii, :parked]) end
            detail[36] = p[ii, :lengths]
            detail[37] = p[ii, :horse_time]
        elseif p[ii, :call]=="Stretch"
            detail[39] = p[ii, :position]
            if !ismissing.(p[ii, :before]) detail[38] = strip(p[ii, :before]) end
            if !ismissing.(p[ii, :after]) detail[40] = strip(p[ii, :after]) end
            if !ismissing.(p[ii, :parked]) detail[41] = strip(p[ii, :parked]) end
            detail[42] = p[ii, :lengths]
        end
    end
    detail
end

# Get Pays from dailies and add to details from Lines
@everywhere function add_pays_to_details(hash, detail)
    detail = [detail;  0.0;0.0;0.0]
    dir = ENV["DATADIR"] * "dailies/$(hash[1])/$(hash[2])/$(hash[3])/tattoos/$(detail[2])/"
    if !isfile(dir*"pays.csv") return detail end
    p = CSV.read(dir*"pays.csv")
    if nrow(p)==0 return detail end

    for ii in 1:nrow(p)
        if p[ii, :wager]=="WIN" detail[49] = p[ii, :pays] end
        if p[ii, :wager]=="PLACE" detail[50] = p[ii, :pays] end
        if p[ii, :wager]=="SHOW" detail[51] = p[ii, :pays] end
    end
    detail
end

# Get Finish info from dailies and add to details of Lines
@everywhere function add_finish_to_details(hash, detail)
    detail = [detail;  "-";0;0;0.0;0.0;0.0]
    dir = ENV["DATADIR"] * "dailies/$(hash[1])/$(hash[2])/$(hash[3])/tattoos/$(detail[2])/"
    if !isfile(dir*"finish.csv") return detail end
    f = []
    open(dir*"finish.csv") do file f = split(strip(read(file, String)), ",") end
    if length(f)<=1 return detail end
    detail[43] = f[2] == "" ? "-" : strip(f[2])
    detail[44] = parse(Int64, f[3])
    detail[45] = parse(Int64, f[4])
    detail[46] = parse(Float64, f[6])
    detail[47] = parse(Float64, f[7])
    detail[48] = f[8] == "" ? 0.0: parse(Float64, f[8])
    detail
end

# Get ratings from dailies and add to details of Lines
@everywhere function add_ratings_to_details(hash, detail)
    detail = [detail;  0.0;0.0;0.0;0.0;0.0]
    dir = ENV["DATADIR"] * "dailies/$(hash[1])/$(hash[2])/$(hash[3])/tattoos/$(detail[2])/"
    if !isfile(dir*"ratings.csv") return detail end
    r = []
    open(dir*"ratings.csv") do file r = split(strip(read(file, String)), ",") end
    if length(r)<=1 return detail end
    detail[52] =  parse(Float64, r[1])
    detail[53] =  parse(Float64, r[2])
    detail[54] =  parse(Float64, r[3])
    detail[55] =  parse(Float64, r[4])
    detail[56] =  parse(Float64, r[5])
    detail
end

# Get lines (details on each horse in a race) from dailies
# and append to main Lines file.
@everywhere function add_lines(hash)
    dir = ENV["DATADIR"] * "dailies/$(hash[1])/$(hash[2])/$(hash[3])/"
    fid1 = open(ENV["DATADIR"]*"lines.csv", "a")
    l = CSV.read(dir*"lines.csv")
    sort!(l, [:post])

    for ii in 1:nrow(l)
        mline = l[ii,:m_line]=="-"? "0": "$(l[ii,:m_line])"
        try split(mline, r"/")
        catch
            println(l[ii,:])
        end
        #TODO mline = try split(mline, r"/"")
        mline = split(mline, r"/")
        mline = length(mline)==1?parse(Float64,mline[1]):parse(Float64,mline[1])/parse(Float64,mline[2])
        fav = ismissing.(l[ii,:fav])? "-": l[ii,:fav]
        coupled = ismissing.(l[ii,:coupled])? "-": l[ii,:coupled]
        dnum = ismissing.(l[ii,:dnum])? "-": "$(l[ii,:dnum])"
        tnum = ismissing.(l[ii,:tnum])? "-": "$(l[ii,:tnum])"
        med = ismissing.(l[ii,:med])? "-": l[ii,:med]
        equip = ismissing.(l[ii,:equip])? "-": l[ii,:equip]
        claim = ismissing.(l[ii,:claim])? "-": l[ii,:claim]
        head = ismissing.(l[ii,:head])? "$(l[ii,:post])": "$(l[ii,:head])"
        detail = [hash[4],l[ii,:tattoo],head,l[ii,:post],
            l[ii,:earns],l[ii,:pays],fav,coupled,dnum,tnum,
            med,equip,claim,l[ii,:c_price],mline,l[ii,:r],l[ii,:pp]]
        detail = add_points_to_details(hash, detail)
        detail = add_finish_to_details(hash, detail)
        detail = add_pays_to_details(hash, detail)
        detail = add_ratings_to_details(hash, detail)

        write(fid1, join(detail,",")*"\n")
        newhash = [hash[1], hash[2], hash[3], l[ii,:tattoo], hash[4]]
        update_tattoo!(newhash)
    end
    close(fid1)
end

# If race results have been processed by trackmaster.jl process,
# exit function.
# Otherwise, open daily race, times, wps, and exotics, construct
# a raceline detail array, save raceline.
@everywhere function add_raceline_exotics_times_wps(hash)
    fid1 = open(ENV["DATADIR"]*"racelines.csv", "a")
    dir = ENV["DATADIR"] * "dailies/$(hash[1])/$(hash[2])/$(hash[3])/"

    # If race results are already calculated, exit function.
    # TrackMaster.jl sets race.csv field 15 to T when it processes results for that race.
    # If there were PP entries but no results, the 15th field would be "F"
    r = []
    open(dir*"race.csv") do f r = split(strip(read(f, String)), ",") end
    if r[15]!="T" return end

    t = []
    open(dir*"times.csv") do f t = split(strip(read(f, String)), ",") end

    # Retrieve the WPS info from the dailies
    w = CSV.read(dir*"wps.csv"; datarow=1)

    # Retrieve exotics data from dailies
    e = get_exotics(hash)

    # Construct raceline object
    detail = hash[4],hash[1],hash[2],hash[3],r[4],r[2],parse(Int64,r[1]),parse(Int64,r[8]),parse(Int64,r[7]),
        parse(Int64,r[3]),r[6],parse(Int64,r[9]),parse(Int64,r[10]),0,parse(Float64,r[5]),
        parse(Float64,t[1]),parse(Float64,t[2]),parse(Float64,t[3]),parse(Float64,t[4]),
        e[1],e[2],e[3],e[4],e[5],e[6],e[7],e[8],e[9],e[10],e[11],e[12],e[13],e[14],e[15],
        e[16],e[17],e[18],e[19],e[20],e[21],e[22],e[23],e[24],e[25],e[26],e[27],e[28],
        e[29],e[30],e[31],e[32],e[33],e[34],e[35],e[36],e[37],e[38],e[39],e[40],
        w[1,:1],w[1,:2],w[1,:3],w[1,:4],
        parse(Float64,r[11]),parse(Int64,r[13]),parse(Int64,r[12]),r[14],r[16],r[15]
    # Append to racelines.csv file
    write(fid1, join(detail,",")*"\n")
    close(fid1)
    # Now add the raceline to the dailies
    add_lines(hash)
end

@everywhere function process_races(sd::Int64, ed::Int64)
    dir = ENV["DATADIR"]
    races = CSV.read(dir*"races.csv")
    sort!(races)
    # Get existing raceline or new (empty) one.
    racelines = load_racelines()

    # Loop thru all races, looking for races within start and end date
    # For each race that is within date range,
    #   find a raceline that matches that race's date, track, and race number.
    #   If raceline exists, do nothing.
    #   If raceline does not exist,
    for ii in 1:nrow(races)
        if races[ii, :date]<sd continue
        elseif races[ii, :date]>ed break end
        indexes = find(racelines[:Date] .== races[ii, :date])
        indexes = find(racelines[indexes, :Track] .== races[ii, :track])
        indexes = find(racelines[indexes, :Race] .== races[ii, :race])
        if isempty(indexes)
            hash = [races[ii, :date], races[ii, :track], races[ii, :race], ii]
            # Add a raceline to the dailies
            add_raceline_exotics_times_wps(hash)
        end
    end
end

#################################################################
# build race data program

#SD = parse(Int64, ARGS[1])
#ED = parse(Int64, ARGS[2])
SD = 20180712
#ED = 20140101
ED = 20180713

process_races(SD, ED)
