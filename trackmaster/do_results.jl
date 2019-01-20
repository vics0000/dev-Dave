# using Distributed # 0.7 code
# do_results.jl

# Define a callback function to receive results from TrackMaster.
@everywhere function cb_results_obj!(chart::Chart, data::Tables)
    data.source=="pps" ? process_chart!(chart, data) : process_races!(chart, data)
end

##############################################################################
# Functions to process the various types from TrackMaster

@everywhere function add_horse!(hash, horse::Starter, data::Tables)
    tattoo = horse.registration_num
    data.horses[tattoo] = horseDetails()
    data.horses[tattoo].name = horse.horse_name
    data.horses[tattoo].horse_sex = horse.horse_info.horse_sex
    data.horses[tattoo].horse_sire = horse.horse_info.horse_sire
    data.horses[tattoo].horse_dam = horse.horse_info.horse_dam
    data.horses[tattoo].horse_damsire = horse.horse_info.horse_damsire
    update_tattoo!(hash)
end

@everywhere function add_trainer!(trainer::Trainer, data::Tables)
    info = split(trainer.trainer_name, ['\n'])
    data.trainers[strip(info[2])] = strip(info[3])
end

@everywhere function add_trainer_pp!(trainer::Trainer, data::Tables)
    data.trainers[trainer.trainer_key] = trainer.trainer_name
end

@everywhere function add_driver_pp!(driver::Driver, data::Tables)
    data.drivers[driver.driver_key] = driver.driver_name
end

@everywhere function add_driver!(driver::Driver, data::Tables)
    info = split(driver.driver_name, ['\n'])
    data.drivers[strip(info[2])] = strip(info[3])
end

@everywhere function add_line!(hash, horse::Starter, data::Tables)
    date = hash[1]
    track = hash[2]
    race = hash[3]
    tattoo = hash[4]
    if get(data.lines_by_date[date].tracks[track].races[race].lines, tattoo, 0)==0
        data.lines_by_date[date].tracks[track].races[race].lines[tattoo] = Starter()
    end
    data.lines_by_date[date].tracks[track].races[race].lines[tattoo] = horse
end

@everywhere function build_horse!(horse::Starter)
    horse.dnum = strip(split(horse.driver.driver_name, ['\n'])[2])
    horse.tnum = strip(split(horse.trainer.trainer_name, ['\n'])[2])
    horse.post = parse(Int64, horse.point_of_calls[1].position)
end

@everywhere function process_lines!(hash, race::Race, data::Tables)
    update = false
    r, p = load_lines!(hash, data)

    for ii in keys(race.starters)
        horse = race.starters[ii]
        tattoo = horse.registration_num
        hashh = [hash; tattoo]
        add_horse!(hashh, horse, data)
        add_trainer!(horse.trainer, data)
        add_driver!(horse.driver, data)
        build_horse!(horse)
        add_line!(hashh, horse, data)
    end
    if r=="F" update_lines(hash, data) end
end

@everywhere function add_track!(chart::Chart, data::Tables)
    code = strip(chart.track.code)
    data.tracks[code] = Track()
    data.tracks[code].code = code
    data.tracks[code].name = chart.track.name
    data.tracks[code].size = chart.track.size
end

@everywhere function build_horse_pp(hash, horse::Int64, chart::Chart)
    race = hash[3]
    horse = chart.trackdata.racedata[race].starters[horse]
    details = horse.details
    horse.registration_num = details["regno"]
    horse.horse_name = details["horse_name"]
    horse.horse_info.horse_sex = details["sex"]
    horse.horse_info.horse_sire = details["sire"]
    horse.horse_info.horse_dam = details["dam"]
    horse.horse_info.horse_damsire = chart.dams[details["dam"]].details["damsire"]
    horse.trainer.trainer_key = details["trainer"]
    horse.trainer.trainer_name = chart.trainers[details["trainer"]].details["trainname"]
    horse.driver.driver_key = details["driver"]
    horse.driver.driver_name = chart.drivers[details["driver"]].details["drivername"]
    horse.dnum = horse.driver.driver_key
    horse.tnum = horse.trainer.trainer_key
    horse.post = parse(Int64, details["pp"])
    horse.program_number = details["program"]
    horse.claimprice = parse(Float64, details["claimprice"])
    horse.m_line = details["morn_odds"]
    horse.equipment = details["me_hopfree"]
    horse.medication = details["me_lasixto"] * details["me_buteto"]
    horse
end

@everywhere function add_starter_pp!(hash, horse::Int64, chart::Chart, data::Tables)
    date = hash[1]
    track = hash[2]
    race = hash[3]
    entry = chart.trackdata.racedata[race].starters[horse]
    details = entry.details
    starter = build_horse_pp(hash, horse, chart)
    hashh = [hash; details["regno"]]
    add_horse!(hashh, starter, data)
    add_trainer_pp!(starter.trainer, data)
    add_driver_pp!(starter.driver, data)
    add_line!(hashh, starter, data)
end

@everywhere function process_races!(chart::Chart, data::Tables)
    date = parse(Int64, replace(chart.race_date, "-", ""))
    code = strip(chart.track.code)
    add_track!(chart, data)
    parse_races!(chart, data)
end

@everywhere function process_chart!(chart::Chart, data::Tables)
    date = parse(Int64, chart.race_date)
    code = strip(chart.trackdata.track)
    chart.track.code = code
    add_track!(chart, data)
    load_races!(chart, data)

    for ii in keys(chart.trackdata.racedata)
        race = chart.trackdata.racedata[ii]
        hash = [date, code, ii]
        r, p = load_lines!(hash, data)
        for jj in keys(race.starters) add_starter_pp!(hash, jj, chart, data) end
        if p=="F" update_lines_pp(hash, data) end
    end
end
