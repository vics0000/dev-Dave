using LightXML
# using Distributed# 0.7 code

@everywhere function parse_trainer!(charts::Chart, node::XMLElement)
    info = Dict{String,String}()
    trkey = ""

    for nd in child_elements(node)
        tag = LightXML.name(nd)
        info[tag] = content(nd)
        if (tag == "trkey")
            trkey = content(nd)
        end
    end
    charts.trainers[trkey] = Trainer()
    charts.trainers[trkey].details = info
    trkey
end

@everywhere function parse_details(node::XMLElement)
    info = Dict{String,String}()

    for nd in child_elements(node)
        tag = LightXML.name(nd)
        info[tag] = content(nd)
    end
    info
end

@everywhere function parse_poststats(node::XMLElement)
    OFFinfo = Dict{String,String}()
    FASTinfo = Dict{String,String}()
    AT_TRACKinfo = Dict{String,String}()

    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "OFF")
            OFFinfo = parse_details(nd)
        elseif (tag == "FAST")
            FASTinfo = parse_details(nd)
        elseif (tag == "AT_TRACK")
            AT_TRACKinfo = parse_details(nd)
        else
            println("Unknown tag in parse_poststats(): ", tag)
        end
    end
    OFFinfo, FASTinfo, AT_TRACKinfo
end

@everywhere function parse_driver!(charts::Chart, node::XMLElement)
    info = Dict{String,String}()
    OFFinfo = Dict{String,String}()
    FASTinfo = Dict{String,String}()
    AT_TRACKinfo = Dict{String,String}()
    drkey = ""

    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "drkey")
            drkey = content(nd)
            info[tag] = content(nd)
        elseif (tag == "poststats")
            OFFinfo, FASTinfo, AT_TRACKinfo = parse_poststats(nd)
        else
            info[tag] = content(nd)
        end
    end
    charts.drivers[drkey] = Driver()
    charts.drivers[drkey].details = info
    charts.drivers[drkey].OFF = OFFinfo
    charts.drivers[drkey].FAST = FASTinfo
    charts.drivers[drkey].AT_TRACK = AT_TRACKinfo
    drkey
end

@everywhere function parse_sire!(charts::Chart, node::XMLElement)
    info = Dict{String,String}()
    name = ""

    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "sirename")
            name = content(nd)
            info[tag] = name
        else
            info[tag] = content(nd)
        end
    end
    charts.sires[name] = Horse()
    charts.sires[name].details = info
    name
end

#parse sires and parse dams almost identical methods
@everywhere function parse_dam!(charts::Chart, node::XMLElement)
    info = Dict{String,String}()
    name = ""

    for nd in child_elements(node)
        tag = LightXML.name(nd)

        #Could be rewritten in 2 lines as
        #info[tag] = content(nd)
        #if tag == "damname"; name = info[tag] end
        #See trainer details at top

        if (tag == "damname")
            name = content(nd)
            info[tag] = name
        else
            info[tag] = content(nd)
        end
    end
    charts.dams[name] = Horse()
    charts.dams[name].details = info
    name
end

@everywhere function parse_horsedata!(charts::Chart, race::Int64, horse::Int64, node::XMLElement)
    lines = 0

    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "trainer")
            trkey = parse_trainer!(charts, nd)
            charts.trackdata.racedata[race].starters[horse].details[tag] = trkey
        elseif (tag == "driver")
            drkey = parse_driver!(charts, nd)
            charts.trackdata.racedata[race].starters[horse].details[tag] = drkey
        elseif (tag == "sire")
            sire = parse_sire!(charts, nd)
            charts.trackdata.racedata[race].starters[horse].details[tag] = sire
        elseif (tag == "dam")
            dam = parse_dam!(charts, nd)
            charts.trackdata.racedata[race].starters[horse].details[tag] = dam
        elseif (tag == "ppdata")
            lines += 1
            push!(charts.trackdata.racedata[race].starters[horse].lines, Horse())
            charts.trackdata.racedata[race].starters[horse].lines[lines].details = parse_details(nd)
        else
            charts.trackdata.racedata[race].starters[horse].details[tag] = content(nd)
        end
    end
end

@everywhere function parse_racedata!(charts::Chart, node::XMLElement)

    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "horsedata")
            push!(charts.trackdata.racedata[charts.racenum].starters, Starter())
            horse = length(charts.trackdata.racedata[charts.racenum].starters)
            parse_horsedata!(charts, charts.racenum, horse, nd)
        elseif (tag == "track")
        elseif (tag == "simulcast")
        elseif (tag == "race")
            charts.racenum += 1
            push!(charts.trackdata.racedata, Race())
            charts.trackdata.racedata[charts.racenum].number = parse(Int64, content(nd))
        else
            charts.trackdata.racedata[charts.racenum].racedata[tag] = content(nd)
        end
    end
end

@everywhere function parse_trackdata!(charts::Chart, node::XMLElement)

    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "track")
            charts.trackdata.track = content(nd)
        elseif (tag == "send_track")
            charts.trackdata.send_track = content(nd)
        elseif (tag == "racedata")
            parse_racedata!(charts, nd)
        else
            charts.trackdata.details[tag] = content(nd)
        end
    end
end

@everywhere function parse_pp(date::SubString{String}, node::XMLElement)
    charts = Chart()
    charts.race_date = date

    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "trackdata")
            parse_trackdata!(charts, nd)
        else
            println("Unknown tag in parse_pp(): ", tag)
        end
    end
    charts
end

function getxmldoc(xmlfile::String)
end


function pp_parse_file(data::Tables, dir::String, xmlfile::String)
    println("DEBUG alt_pp_parse.jl pp_parse_file line 202 $xmlfile")
    date = match(r"[0-9]+", xmlfile)
    date = date.match
    println("DEBUG alt_pp_parse.jl pp_parse_file line 205 $date")
    xmldoc = try parse_file(dir*xmlfile)
    catch error
        println(error)
        println(xmlfile)
        return
    end
    chart = parse_pp(date, LightXML.root(xmldoc))


    free(xmldoc)
    cb_chart(chart, data)
end
