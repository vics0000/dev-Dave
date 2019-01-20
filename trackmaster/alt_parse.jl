using LightXML
# using Distributed# 0.7 code

@everywhere function parse_chart(node::XMLElement)
    chart = Chart()
    chart.race_date = attribute(node, "RACE_DATE")
    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "TRACK")
            chart.racenum += 1
            chart.track = parse_track(nd)
         elseif (tag == "RACE")
            push!(chart.races, parse_race(nd))
        end
    end
    chart
end

@everywhere function parse_track(node::XMLElement)
    track = Track()
    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "CODE")
            track.code = content(nd)
        elseif (tag == "NAME")
            track.name = content(nd)
        elseif (tag == "TRACK_SIZE")
            track.size = content(nd)
        end
    end
    track
end

@everywhere function parse_race(node::XMLElement)
    race = Race()
    race.number = parse(Int64, attribute(node, "NUMBER"))
    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if tag == "CARD_ID"
            race.card_id = content(nd)
        elseif tag == "TYPE"
            race.race_type = content(nd)
        elseif tag == "PURSE"
            race.purse = parse(Float64, content(nd))
        elseif tag == "RACE_GAIT"
            race.race_gait = content(nd)
        elseif tag == "DISTANCE"
            race.distance = parse(Float64, content(nd))
        elseif tag == "TRK_COND"
            race.trk_cond = content(nd)
        elseif tag == "TEMPERATURE"
            race.temperature = parse(Int64, content(nd))
        elseif tag == "FIELD_SIZE"
            race.field_size = parse(Int64, content(nd))
        elseif tag == "RACE_TIMES"
            for o_nd in child_elements(nd)
                o_tag = LightXML.name(o_nd)
                if (o_tag == "RACE_TIME")
                    push!(race.race_times, parse_race_time(o_nd))
                end
            end
        elseif tag == "ITV"
            race.itv = parse(Int64, content(nd))
        elseif tag == "DTV"
            race.dtv = parse(Int64, content(nd))
        elseif tag == "CLASSRTG"
            race.classrtg = parse(Int64, content(nd))
        elseif tag == "CANADIAN_RATE"
            race.canadian_rate = parse(Float64, content(nd))
        elseif tag == "COUNTRY"
            race.country = content(nd)
        elseif tag == "OFF_TIME"
            race.off_time = content(nd)
        elseif tag == "WAGERS"
            race.wagers = parse_wagers(nd)
        elseif tag == "STARTERS"
            for o_nd in child_elements(nd)
                o_tag = LightXML.name(o_nd)
                if (o_tag == "STARTER")
                    push!(race.starters, parse_starter(o_nd))
                end
            end
         end
    end
    race
end

@everywhere function parse_horse_info(node::XMLElement)
    horse_info = HorseInfo()
    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "HORSE_AGE")
            horse_info.horse_age = parse(Int64, content(nd))
        elseif (tag == "HORSE_SEX")
            horse_info.horse_sex = content(nd)
        elseif (tag == "HORSE_SIRE")
            horse_info.horse_sire = content(nd)
        elseif (tag == "HORSE_DAM")
            horse_info.horse_dam = content(nd)
        elseif (tag == "HORSE_DAMSIRE")
            horse_info.horse_damsire = content(nd)
        end
    end
    horse_info
end

@everywhere function parse_point(node::XMLElement)
    point = Point()
    point.call = attribute(node, "Call");
    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "Before")
            point.before = content(nd)
        elseif (tag == "Position")
            point.position = content(nd)
        elseif (tag == "After")
            point.after = content(nd)
        elseif (tag == "Parked")
            point.parked = content(nd)
        elseif (tag == "Lengths")
            point.lengths = content(nd)
        elseif (tag == "HorseTime")
            point.horse_time = content(nd)
        end
    end
    point
end

@everywhere function parse_finishpoc(node::XMLElement)
    finishpoc = FinishPoc()
    finishpoc.call = attribute(node, "Call");
    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "Before")
            finishpoc.before = content(nd)
        elseif (tag == "Original")
            finishpoc.original = content(nd)
        elseif (tag == "Official")
            finishpoc.official = content(nd)
        elseif (tag == "Parked")
            finishpoc.parked = content(nd)
        elseif (tag == "Lengths")
            finishpoc.lengths = content(nd)
        elseif (tag == "HorseTime")
            finishpoc.horse_time = content(nd)
        elseif (tag == "Final_Fraction_Time")
            finishpoc.final_fraction_time = content(nd)
        end
    end
    finishpoc
end

@everywhere function parse_speedpoc(node::XMLElement)
    speedpoc = SpeedPoc()
    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "Pace_First_Fraction")
            speedpoc.pace_first_fraction = parse(Float64, content(nd))
        elseif (tag == "Pace_Second_Fraction")
            speedpoc.pace_second_fraction = parse(Float64, content(nd))
        elseif (tag == "Pace_Third_Fraction")
            speedpoc.pace_third_fraction = parse(Float64, content(nd))
        elseif (tag == "Pace_Final_Fraction")
            speedpoc.pace_final_fraction = parse(Float64, content(nd))
        elseif (tag == "Final_Speed")
            speedpoc.final_speed = parse(Float64, content(nd))
        end
    end
    speedpoc
end

@everywhere function parse_driver(node::XMLElement)
    driver = Driver()
    driver.driver_key = content(node)
    driver.driver_name = content(node)
    driver
end

@everywhere function parse_trainer(node::XMLElement)
    trainer = Trainer()
    trainer.trainer_key = content(node)
    trainer.trainer_name = content(node)
    trainer
end

@everywhere function parse_race_time(node::XMLElement)
    race_time = RaceTime()
    race_time.call = attribute(node, "CALL")
    race_time.time = content(node)
    race_time
end

@everywhere function parse_starter(node::XMLElement)
    starter = Starter()
    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "HORSE_NAME")
            starter.horse_name = content(nd)
        elseif (tag == "REGISTRATION_NUM")
            starter.registration_num = content(nd)
        elseif (tag == "HORSE_GAIT")
            starter.horse_gait = content(nd)
        elseif (tag == "RACE_CLASS")
            starter.race_class = content(nd)
        elseif (tag == "EARNINGS")
            starter.earnings = parse(Float64, content(nd))
        elseif (tag == "HORSE_INFO")
            starter.horse_info = parse_horse_info(nd)
        elseif (tag == "POINT_OF_CALLS")
            for o_nd in child_elements(nd)
                o_tag = LightXML.name(o_nd)
                if (o_tag == "POINT")
                    push!(starter.point_of_calls, parse_point(o_nd))
                end
            end
        elseif (tag == "FINISH")
            for o_nd in child_elements(nd)
                o_tag = LightXML.name(o_nd)
                if (o_tag == "FINISHPOC")
                    push!(starter.finish, parse_finishpoc(o_nd))
                end
            end
        elseif (tag == "SPEED_FIGURES")
            for o_nd in child_elements(nd)
                o_tag = LightXML.name(o_nd)
                if (o_tag == "SPEEDPOC")
                    push!(starter.speed_figures, parse_speedpoc(o_nd))
                end
            end
        elseif (tag == "ODDSTOADOLLAR")
            starter.oddstoadollar = parse(Float64, content(nd))
        elseif (tag == "FAVORITE")
            starter.favorite = content(nd)
        elseif (tag == "COUPLED")
            starter.coupled = content(nd)
        elseif (tag == "DRIVER")
            starter.driver = parse_driver(nd)
        elseif (tag == "TRAINER")
            starter.trainer = parse_trainer(nd)
        elseif (tag == "MEDICATION")
            starter.medication = content(nd)
        elseif (tag == "EQUIPMENT")
            starter.equipment = content(nd)
        elseif (tag == "RACE_COMMENTS")
            starter.race_comments = content(nd)
        elseif (tag == "CLAIM_INDICATOR")
           starter.claim_indicator = content(nd)
        elseif (tag == "CLAIMPRICE")
            starter.claimprice = parse(Float64, content(nd))
        elseif (tag == "PROGRAM_NUMBER")
            starter.program_number = content(nd)
        elseif (tag == "PAYOUTS")
            for o_nd in child_elements(nd)
                o_tag = LightXML.name(o_nd)
                if (o_tag == "PAYOUT")
                    payout = Payout()
                    payout.pos = attribute(o_nd, "POS")
                    payout.amount = parse(Float64, content(o_nd))
                    push!(starter.payouts, payout)
                end
            end
        end
    end
    starter
end

@everywhere function parse_wps(node::XMLElement)
    wps = WPS()
    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "WIN_POOL")
            wps.win_pool = parse(Float64, content(nd))
        elseif (tag == "PLACE_POOL")
            wps.place_pool = parse(Float64, content(nd))
        elseif (tag == "SHOW_POOL")
            wps.show_pool = parse(Float64, content(nd))
        elseif (tag == "TOTAL_POOL")
            wps.total_pool = parse(Float64, content(nd))
        end
    end
    wps
end

@everywhere function parse_exotic(node::XMLElement)
    exotic = Exotic()
    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "WAGER")
            exotic.wager = Wager()
            try exotic.wager.type_key = attribute(nd, "TYPE_KEY")
            catch
            end
            exotic.wager.wager = content(nd)
        elseif (tag == "RESULT_STRING")
            exotic.result_string = content(nd)
        elseif (tag == "PAYOFFS")
            for o_nd in child_elements(nd)
                o_tag = LightXML.name(o_nd)
                if (o_tag == "PAYOFF")
                    push!(exotic.payoffs, parse(Float64, content(o_nd)))
                end
            end
        elseif (tag == "CARRYOVER")
            exotic.carryover = parse(Float64, content(nd))
        elseif (tag == "BASE_WAGER")
            exotic.base_wager = parse(Float64, content(nd))
        elseif (tag == "POOL_TOTAL")
            exotic.pool_total = parse(Float64, content(nd))
        elseif (tag == "CANCELLED")
            exotic.cancelled = content(nd)
        elseif (tag == "NOT_PAID")
            exotic.not_paid = content(nd)
        elseif (tag == "REFUNDED")
            exotic.refunded = content(nd)
        end
    end
    exotic
end

@everywhere function parse_wagers(node::XMLElement)
    wagers = Wagers()
    for nd in child_elements(node)
        tag = LightXML.name(nd)
        if (tag == "WPS")
            wagers.wps = parse_wps(nd)
        elseif (tag == "EXOTICS")
            for o_nd in child_elements(nd)
                o_tag = LightXML.name(o_nd)
                if (o_tag == "EXOTIC")
                    push!(wagers.exotics, parse_exotic(o_nd))
                end
            end
        end
    end
    wagers
end

@everywhere function alt_parse_file(data::Tables, xmlfile::String)
    xmldoc = parse_file(xmlfile)
    chart = parse_chart(LightXML.root(xmldoc))
    free(xmldoc)
    cb_chart(chart, data)
end
