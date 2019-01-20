# amt_xml.jl
# This file contains functions to parse through an XML block from the AMT log file.

using LightXML

# Entry point to parse XML from an AMT log file.

function amt_parse_xml(xml)
    obj = nothing
    xmldoc = xml[end]=='>'? parse_string(xml): return obj
    node = LightXML.root(xmldoc)
    tag = name(node)
    if (tag == "gpAvailablePrograms")
        obj = parse_xml_gpAvailablePrograms(node)
    elseif (tag == "gpCycleData")
        obj = parse_xml_gpCycleData(node)
    elseif (tag == "gpItpData")
        obj = parse_xml_gpItpData(node)
    elseif (tag == "gpPoolHolder")
        obj = parse_xml_gpPoolHolder(node)
    elseif (tag == "gpPrices")
        obj = parse_xml_gpPrices(node)
    elseif (tag == "gpProgramCombine")
        obj = parse_xml_gpProgramCombine(node)
    elseif (tag == "gpRaceDefinition")
        obj = parse_xml_gpRaceDefinition(node)
    elseif (tag == "gpWillPays")
        obj = parse_xml_gpWillPays(node)
    elseif (tag == "gpRequestAvailablePoolSpelling")
        obj = "Found a gpRequestAvailablePoolSpelling record"
    else
        println("Unhandled $tag at top-level.")
    end
    obj
end

# Utility functiond to convert a text-comma-list to an array

function comma_list_to_array(a, s)
    for ns in split(s, ',')
        if (ns != "")
            push!(a, ns)
        end
    end
end

function comma_list_to_int_array(a, s)
    for ns in split(s, ',')
        if (ns != "")
            push!(a, parse(Int64, ns))
        end
    end
end

# The remainder of this program is for parsing XML structures.

function parse_xml_BetTypeDefinition(node)
    bt = BetTypeDefinition()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "betTypeName")
            bt.betTypeName = content(nd)
        elseif (tag == "validPermutations")
            bt.validPermutations = content(nd)
        end
    end
    bt
end

function parse_xml_BetTypeInformation(node)
    bti = BetTypeInformation()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "raceNumber")
            bti.raceNumber = parse(Int64, content(nd))
        elseif (tag == "betTypeName")
            bti.betTypeName = content(nd)
        elseif (tag == "validPermutations")
            bti.validPermutations = content(nd)
        elseif (tag == "numberOfAdditionalLegs")
            bti.numberOfAdditionalLegs = parse(Int64, content(nd))
        elseif (tag == "legList")
            comma_list_to_int_array(bti.legList, content(nd))
        elseif (tag == "minBaseAmount")
            bti.minBaseAmount = parse(Float64, content(nd))
        elseif (tag == "maxBaseAmount")
            bti.maxBaseAmount = parse(Float64, content(nd))
        elseif (tag == "minBoxAmount")
            bti.minBoxAmount = parse(Float64, content(nd))
        elseif (tag == "minWheelAmount")
            bti.minWheelAmount = parse(Float64, content(nd))
        elseif (tag == "maxPermuteAmount")
            bti.maxPermuteAmount = parse(Float64, content(nd))
        elseif (tag == "multipleAmount")
            bti.multipleAmount = parse(Float64, content(nd))
        elseif (tag == "error")
        elseif (tag == "errorCode")
        elseif (tag == "numberOfRunnerGroups")
        else
            println("Unhandled $tag in BetTypeInformation. Content: ", content(nd))
        end
    end
    bti
end

function parse_xml_gpAvailablePrograms(node)
    avprog = GpAvailablePrograms()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "numberOfPrograms")
            avprog.numberOfPrograms = parse(Int64, content(nd))
        elseif (tag == "programList")
            for o_nd in child_elements(nd)
                o_tag = name(o_nd)
                if (o_tag == "ProgramInfo")
                    push!(avprog.programList, parse_xml_programInfo(o_nd))
                end
            end
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in gpAvailablePrograms.")
        end
    end
    avprog
end

function parse_xml_gpCycleData(node)
    cycdata = GpCycleData()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "programName")
            cycdata.programName = content(nd)
        elseif (tag == "race")
            cycdata.race = parse(Int64, content(nd))
        elseif (tag == "source")
            cycdata.source = parse(Int64, content(nd))
        elseif (tag == "odds")
            for o_nd in child_elements(nd)
                o_tag = name(o_nd)
                if (o_tag == "gpOdds")
                    push!(cycdata.odds, parse_xml_gpOdds(o_nd))
                end
            end
        elseif (tag == "probs")
            for p_nd in child_elements(nd)
                p_tag = name(p_nd)
                if (p_tag == "gpProbs")
                    push!(cycdata.probs, parse_xml_gpProbs(p_nd))
                end
            end
        elseif (tag == "pools")
            for p_nd in child_elements(nd)
                p_tag = name(p_nd)
                if (p_tag == "gpPool")
                    push!(cycdata.pools, parse_xml_gpPool(p_nd))
                end
            end
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in gpCycleData.")
        end
    end
    cycdata
end

function parse_xml_gpItpData(node)
    data = GpItpData()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "itpData")
            data.itpData = parse_xml_itpData(nd)
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in gpItpData.")
        end
    end
    data
end

function parse_xml_gpOdds(node)
    odds = GpOdds()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "programName")
            odds.programName = content(nd)
        elseif (tag == "race")
            odds.race = parse(Int64, content(nd))
        elseif (tag == "betType")
            odds.betType = content(nd)
        elseif (tag == "source")
            odds.source = parse(Int64, content(nd))
        elseif (tag == "cycleType")
            odds.cycleType = content(nd)
        elseif (tag == "nrRows")
            odds.nrRows = parse(Int64, content(nd))
        elseif (tag == "nrValuesPerRow")
            odds.nrValuesPerRow = parse(Int64, content(nd))
        elseif (tag == "odds")
            for o_nd in child_elements(nd)
                push!(odds.odds, content(o_nd))
            end
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in gpOdds.")
        end
    end
    odds
end

function parse_xml_gpPool(node)
    pool = GpPool()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "programName")
            pool.programName = content(nd)
        elseif (tag == "race")
            pool.race = parse(Int64, content(nd))
        elseif (tag == "betType")
            pool.betType = content(nd)
        elseif (tag == "source")
            pool.source = parse(Int64, content(nd))
        elseif (tag == "cycleType")
            pool.cycleType = content(nd)
        elseif (tag == "poolTotal")
            pool.poolTotal = content(nd)
        elseif (tag == "nrRows")
            pool.nrRows = parse(Int64, content(nd))
        elseif (tag == "nrValuesPerRow")
            pool.nrValuesPerRow = parse(Int64, content(nd))
        elseif (tag == "money")
            for o_nd in child_elements(nd)
                push!(pool.money, content(o_nd))
            end
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in gpPool.")
        end
    end
    pool
end

function parse_xml_gpPoolHolder(node)
    ph = GpPoolHolder()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "programName")
            ph.programName = content(nd)
        elseif (tag == "race")
            ph.race = parse(Int64, content(nd))
        elseif (tag == "poolHolder")
            for o_nd in child_elements(nd)
                o_tag = name(o_nd)
                if (o_tag == "pHolderlist")
                    for oo_nd in child_elements(o_nd)
                        oo_tag = name(oo_nd)
                        if (oo_tag == "PoolHolder")
                            push!(ph.poolHolders, parse_xml_poolHolder(oo_nd))
                        end
                    end
                end
            end
        elseif (tag == "source")
            ph.source = parse(Int64, content(nd))
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in gpPoolHolder.")
        end
    end
    ph
end

function parse_xml_gpPrices(node)
    prices = GpPrices()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "programName")
            prices.programName = content(nd)
        elseif (tag == "race")
            prices.race = parse(Int64, content(nd))
        elseif (tag == "source")
            prices.source = parse(Int64, content(nd))
        elseif (tag == "orderOfFinish")       # Here we are not going to spec, strictly speaking. But there's seems to be some
            for o_nd in child_elements(nd)    # redundant structure, so we're just trying to simplify.
                o_tag = name(o_nd)
                if (o_tag == "OrderOfFinish")
                    prices.orderOfFinish = content(o_nd)
                end
            end
        elseif (tag == "priceInfo")
            prices.priceInfo = parse_xml_priceInfo(nd)
        elseif (tag == "prices")
            for o_nd in child_elements(nd)
                o_tag = name(o_nd)
                if (o_tag == "priceRecords")
                    for r_nd in child_elements(o_nd)
                        r_tag = name(r_nd)
                        if (r_tag == "PriceRecord")
                            push!(prices.prices, parse_xml_priceRecord(r_nd))
                        end
                    end
                end
            end
        elseif (tag == "date")
            prices.date = content(nd)
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in gpPrices.")
        end
    end
    prices
end

function parse_xml_gpProbs(node)
    probs = GpProbs()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "programName")
            probs.programName = content(nd)
        elseif (tag == "race")
            probs.race = parse(Int64, content(nd))
        elseif (tag == "betType")
            probs.betType = content(nd)
        elseif (tag == "source")
            probs.source = parse(Int64, content(nd))
        elseif (tag == "cycleType")
            probs.cycleType = content(nd)
        elseif (tag == "baseAmount")
            probs.baseAmount = content(nd)
        elseif (tag == "nrRows")
            probs.nrRows = parse(Int64, content(nd))
        elseif (tag == "nrValuesPerRow")
            probs.nrValuesPerRow = parse(Int64, content(nd))
        elseif (tag == "probs")
            for o_nd in child_elements(nd)
                push!(probs.probs, content(o_nd))
            end
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in gpProbs.")
        end
    end
    probs
end

function parse_xml_gpProgramCombine(node)
    pc = GpProgramCombine()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "detail")
            pc.detail = parse_xml_programDetail(nd)
        elseif (tag == "definition")
            pc.definition = parse_xml_programDefinition(nd)
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in gpProgramCombine.")
        end
    end
    pc
end

function parse_xml_gpRaceDefinition(node)
    gprd = GpRaceDefinition()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "programName")
            gprd.programName = content(nd)
        elseif (tag == "raceDetails")
            gprd.raceDetails = parse_xml_raceDetails(nd)
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in gpRaceDefinition.")
        end
    end
    gprd
end

function parse_xml_gpWillPays(node)
    wp = WillPays()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "willPays")
            wp = parse_xml_willPays(nd)
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in gpWillPays.")
        end
    end
    wp
end

function parse_xml_itpData(node)
    data = ItpData()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "Track")
            data.track = parse_xml_track(nd)
        elseif (tag == "Race")
            data.race = parse_xml_race(nd)
        elseif (tag == "Starters")
            for o_nd in child_elements(nd)
                o_tag = name(o_nd)
                if (o_tag == "Selection")
                    push!(data.starters, parse_xml_selection(o_nd))
                end
            end
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in itpData.")
        end
    end
    data
end

function parse_xml_lateResult(node)
    lr = LateResult()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "result")
            lr.result = content(nd)
        elseif (tag == "reason")
            lr.reason = content(nd)
        elseif (tag == "theValue")
            lr.theValue = content(nd)
        end
    end
    lr
end

function parse_xml_poolHolder(node)
    ph = PoolHolder()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "poolCode")
            ph.poolCode = content(nd)
        elseif (tag == "contributorType")
            ph.contributorType = content(nd)
        else
            println("Unhandled $tag in poolHolder.")
        end
    end
    ph
end

function parse_xml_priceInfo(node)
    info = PriceInfo()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "Scratches")
            info.scratches = content(nd)
        elseif (tag == "RaceTime")
            info.raceTime = content(nd)
        elseif (tag == "ToteFavorite")
            info.toteFavorite = content(nd)
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in priceInfo.")
        end
    end
    info
end

function parse_xml_priceRecord(node)
    pr = PriceRecord()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "poolID")
            pr.poolID = content(nd)
        elseif (tag == "baseValue")
            pr.baseValue = length(strip(content(nd)))>0?parse(Float64, content(nd)):0
        elseif (tag == "name")
            pr.name = content(nd)
        elseif (tag == "results")
            pr.results = content(nd)
        elseif (tag == "reasonX")
            pr.reasonX = content(nd)
        elseif (tag == "reasonY")
            pr.reasonY = content(nd)
        elseif (tag == "reasonZ")
            pr.reasonZ = content(nd)
        elseif (tag == "paid")
            pr.paid = length(strip(content(nd)))>0?parse(Float64, content(nd)):0
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in priceRecord.")
        end
    end
    pr
end

function parse_xml_programDefinition(node)
    def = GpProgramDefinition()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "programName")
            def.programName = content(nd)
        elseif (tag == "numberOfBetTypes")
            def.numberOfBetTypes = parse(Int64, content(nd))
        elseif (tag == "betTypeList")
            for o_nd in child_elements(nd)
                o_tag = name(o_nd)
                if (o_tag == "BetTypeDefinition")
                    push!(def.betTypeList, parse_xml_BetTypeDefinition(o_nd))
                end
            end
        elseif (tag == "numberOfRaces")
            def.numberOfRaces = parse(Int64, content(nd))
        elseif (tag == "raceDetailList")

            for o_nd in child_elements(nd)
                o_tag = name(o_nd)
                if (o_tag == "RaceDefinition")
                    raceDefinition = parse_xml_RaceDefinition(o_nd)
                    def.raceDetailList[raceDefinition.raceNumber] = raceDefinition
                end
            end
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in programDefinition.")
        end
    end
    def
end

function parse_xml_programDetail(node)
    pd = GpProgramDetail()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "programNumber")
            pd.programNumber = parse(Int64, content(nd))
        elseif (tag == "programName")
            pd.programName = content(nd)
        elseif (tag == "programLongName")
            pd.programLongName = content(nd)
        elseif (tag == "programDate")
            pd.programDate = content(nd)
        elseif (tag == "programITWDate")
            pd.programITWDate = content(nd)
        elseif (tag == "maxRunners")
            pd.maxRunners = parse(Int64, content(nd))
        elseif (tag == "programType")
            pd.programType = content(nd)
        elseif (tag == "programState")
            pd.programState = content(nd)
        elseif (tag == "zipCodeOfTrack")
            pd.zipCodeOfTrack = content(nd)
        elseif (tag == "alternateDisplayName")
            pd.alternateDisplayName = content(nd)
        elseif (tag == "countryCode")
            pd.countryCode = content(nd)
        elseif (tag == "chan")
            pd.chan = parse(Int64, content(nd))
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in programDetail.")
        end
    end
    pd
end

function parse_xml_RaceDefinition(node)
    rd = RaceDefinition()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "raceNumber")
            rd.raceNumber = parse(Int64, content(nd))
        elseif (tag == "featureNumber")
            rd.featureNumber = parse(Int64, content(nd))
        elseif (tag == "liveRunners")
            comma_list_to_int_array(rd.liveRunners, content(nd))
        elseif (tag == "scratchedRunners")
            comma_list_to_int_array(rd.scratchedRunners, content(nd))
        elseif (tag == "openBetTypes")
            comma_list_to_array(rd.openBetTypes, content(nd))
        elseif (tag == "betTypeInformation")
            for o_nd in child_elements(nd)
                o_tag = name(o_nd)
                if (o_tag == "BetTypeInformation")
                    push!(rd.betTypeInformation, parse_xml_BetTypeInformation(o_nd))
                end
            end
        end
    end
    rd
end

function parse_xml_programInfo(node)
    proginfo = ProgramInfo()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "programNumber")
            proginfo.programNumber = parse(Int64, content(nd))
        elseif (tag == "programName")
            proginfo.programName = content(nd)
        elseif (tag == "programDate")
            proginfo.programDate = length(strip(content(nd)))>0?parse(Int64, content(nd)):0
        elseif (tag == "programStatus")
            proginfo.programStatus = content(nd)
        elseif (tag == "programLongName")
            proginfo.programLongName = content(nd)
        elseif (tag == "maxRunners")
            proginfo.maxRunners = parse(Int64, content(nd))
        elseif (tag == "highestRace")
            proginfo.highestRace = parse(Int64, content(nd))
        elseif (tag == "currentRace")
            proginfo.currentRace = parse(Int64, content(nd))
        elseif (tag == "minutesToPost")
            proginfo.minutesToPost = parse(Int64, content(nd))
        elseif (tag == "trackState")
            proginfo.trackState = content(nd)
        elseif (tag == "trackZipCode")
            proginfo.trackZipCode = content(nd)
        elseif (tag == "displayName")
            proginfo.displayName = content(nd)
        elseif (tag == "countryCode")
            proginfo.countryCode = content(nd)
        elseif (tag == "numberOfRaces")
            proginfo.numberOfRaces = parse(Int64, content(nd))
        elseif (tag == "numberOfBetTypes")
            proginfo.numberOfBetTypes = parse(Int64, content(nd))
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in programInfo.")
        end
    end
    proginfo
end

function parse_xml_race(node)
    race = Race()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "RaceDate")
            race.raceDate = parse(Int64, content(nd))
        elseif (tag == "DayEvening")
            race.dayEvening = content(nd)
        elseif (tag == "RaceNumber")
            race.raceNumber = parse(Int64, content(nd))
        elseif (tag == "BreedType")
            race.breedType = content(nd)
        elseif (tag == "RaceName")
            race.raceName = content(nd)
        elseif (tag == "RaceType")
            race.raceType = content(nd)
        elseif (tag == "Conditions")
            race.conditions = content(nd)
        elseif (tag == "Distance")
            race.distance = content(nd)
        elseif (tag == "PurseUSA")
            race.purseUSA = parse(Float64, content(nd))
        elseif (tag == "Surface")
            race.surface = content(nd)
        elseif (tag == "PostTime")
            race.postTime = content(nd)
        elseif (tag == "WagerText")
            race.wagerText = content(nd)
        elseif (tag == "AgeRestriction")
            ageRestriction = content(nd)
        elseif (tag == "SexRestriction")
            race.sexRestriction = content(nd)
        elseif (tag == "Grade")
            race.grade = content(nd)
        elseif (tag == "Division")
            race.division = content(nd)
        elseif (tag == "Gait")
            race.gait = content(nd)
        elseif (tag == "MaximumClaimingPriceUSA")
            race.maximumClaimingPriceUSA = content(nd)
        elseif (tag == "MinimumClaimingPriceUSA")
            race.minimumClaimingPriceUSA = content(nd)
        elseif (tag == "ProgramSelections")
            race.programSelections = content(nd)
        elseif (tag == "TrackRecord")
            if (has_children(nd))
                push!(race.trackRecord, parse_xml_trackRecord(nd))
            end
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in race.")
        end
    end
    race
end

function parse_xml_raceDetails(node)
    rd = RaceDefinition()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "raceNumber")
            rd.raceNumber = parse(Int64, content(nd))
        elseif (tag == "featureNumber")
            rd.featureNumber = parse(Int64, content(nd))
        elseif (tag == "liveRunners")
            comma_list_to_int_array(rd.liveRunners, content(nd))
        elseif (tag == "scratchedRunners")
            comma_list_to_int_array(rd.scratchedRunners, content(nd))
        elseif (tag == "openBetTypes")
            rd.openBetTypes = split(content(nd), ',')
        elseif (tag == "betTypeInformation") # starts with <betTypeInformation> and a series of <BetTypeInformation> records
            for bt_nd in child_elements(nd)
                bt_tag = name(bt_nd)
                if (bt_tag == "BetTypeInformation")
                    push!(rd.betTypeInformation, parse_xml_BetTypeInformation(bt_nd))
                end
            end
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in raceDetails.")
        end
    end
    rd
end

function parse_xml_raceSummary(node)
    rs = RaceSummary()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "Year")
            rs.year = parse(Int64, content(nd))
        elseif (tag == "Surface")
            rs.surface = content(nd)
        elseif (tag == "NumberOfStarts")
            rs.numberOfStarts = parse(Int64, content(nd))
        elseif (tag == "NumberOfWins")
            rs.numberOfWins = parse(Int64, content(nd))
        elseif (tag == "NumberOfSeconds")
            rs.numberOfSeconds = parse(Int64, content(nd))
        elseif (tag == "NumberOfThirds")
            rs.numberOfThirds = parse(Int64, content(nd))
        elseif (tag == "EarningUSA")
            rs.earningUSA = parse(Float64, content(nd))
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in raceSummary.")
        end
    end
    rs
end

function parse_xml_selection(node)
    sel = Selection()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "Name")
            sel.name = content(nd)
        elseif (tag == "YearOfBirth")
		    try sel.yearOfBirth = parse(Int64, content(nd))
		    catch error
		        println(error)
		    end
        elseif (tag == "FoalingArea")
            sel.foalingArea = content(nd)
        elseif (tag == "BreedType")
            sel.breedType = content(nd)
        elseif (tag == "Color")
            sel.color = content(nd)
        elseif (tag == "Sex")
            sel.sex = content(nd)
        elseif (tag == "HorseDamName")
            sel.horseDamName = content(nd)
        elseif (tag == "HorseSireName")
            sel.horseSireName = content(nd)
        elseif (tag == "BreederName")
            sel.breederName = content(nd)
        elseif (tag == "TrainerName")
            sel.trainerName = content(nd)
        elseif (tag == "OwnerName")
            sel.ownerName = content(nd)
        elseif (tag == "RacingOwnerSilks")
            sel.racingOwnerSilks = content(nd)
        elseif (tag == "JockeyName")
            sel.jockeyName = content(nd)
        elseif (tag == "PostPosition")
		    try sel.postPosition = content(nd)!="AE"?parse(Int64, content(nd)):9
		    catch error
		        println(error)
		        sel.postPosition = 9
		    end
        elseif (tag == "ProgramNumber")
            sel.programNumber = content(nd)
        elseif (tag == "WeightCarried")
            sel.weightCarried = content(nd)
        elseif (tag == "Medication")
            sel.medication = content(nd)
        elseif (tag == "Equipment")
            sel.equipment = content(nd)
        elseif (tag == "MorningLine")
            sel.morningLine = content(nd)
        elseif (tag == "ClaimedPriceUSA")
		    try sel.claimedPriceUSA = parse(Float64, content(nd))
		    catch error
		        println(error)
		    end
        elseif (tag == "TodaysHorseClassRating")
            sel.todaysHorseClassRating = content(nd)
        elseif (tag == "SaddleClothColor")
            sel.saddleClothColor = content(nd)
        elseif (tag == "RaceSummary")
            if (has_children(nd))
                push!(sel.raceSummary, parse_xml_raceSummary(nd))
            end
        elseif (tag == "bad XML") println("$tag in selection.")
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in selection.")
        end
    end
    sel
end

function parse_xml_starters(node)
    starters = Starters()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "")
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
#            println("Unhandled $tag in starters.")
        end
    end
    starters
end

function parse_xml_track(node)
    track = Track()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "TrackID")
            track.trackID = content(nd)
        elseif (tag == "TrackName")
            track.trackName = content(nd)
        elseif (tag == "Country")
            track.country = content(nd)
        elseif (tag == "TimeZone")
            track.timeZone = content(nd)
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in track.")
        end
    end
    track
end

function parse_xml_trackRecord(node)
    trec = TrackRecord()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "RaceDate")
            trec.raceDate = parse(Int64, content(nd))
        elseif (tag == "HorseName")
            trec.horseName = content(nd)
        elseif (tag == "HorseAge")
            trec.horseAge = parse(Int64, content(nd))
        elseif (tag == "BreedType")
            trec.breedType = content(nd)
        elseif (tag == "Sex")
            trec.sex = content(nd)
        elseif (tag == "WinningTime")
            trec.winningTime = parse(Int64, content(nd))
        elseif (tag == "WeightCarried")
            trec.weightCarried = parse(Int64, content(nd))
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in trackRecord.")
        end
    end
    trec
end

function parse_xml_willPays(node)
    wp = WillPays()
    for nd in child_elements(node)
        tag = name(nd)
        if (tag == "programName")
            wp.programName = content(nd)
        elseif (tag == "race")
            wp.race = parse(Int64, content(nd))
        elseif (tag == "betType")
            wp.betType = content(nd)
        elseif (tag == "source")
            wp.source = parse(Int64, content(nd))
        elseif (tag == "baseAmount")
            wp.baseAmount = content(nd)
        elseif (tag == "earlyResult")
            wp.earlyResult = content(nd)
        elseif (tag == "willPayList")
            for o_nd in child_elements(nd)
                o_tag = name(o_nd)
                if (o_tag == "LateResult")
                    push!(wp.willPayList, parse_xml_lateResult(o_nd))
                end
            end
        elseif (tag == "error")
        elseif (tag == "errorCode")
        else
            println("Unhandled $tag in willPays.")
        end
    end
    wp
end
