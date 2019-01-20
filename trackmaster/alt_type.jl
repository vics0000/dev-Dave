using DataFrames
# using Distributed# 0.7 code

mutable struct Track
    code::String
    name::String
    size::String
end

function Track()
    Track("-", "-", "-")
end

mutable struct RaceTime
    call::String # Usually contains an int, but sometimes "FINAL_TIME"
    time::String
end

function RaceTime()
    RaceTime("","")
end

mutable struct WPS
    win_pool::Float64
    place_pool::Float64
    show_pool::Float64
    total_pool::Float64
end

function WPS()
    WPS(0.0,0.0,0.0,0.0)
end

mutable struct Wager
    type_key::String
    wager::String
end

function Wager()
    Wager("","")
end

mutable struct Exotic
    wager::Wager
    result_string::String
    payoffs::Array{Float64}
    carryover::Float64
    base_wager::Float64
    pool_total::Float64
    cancelled::String
    not_paid::String
    refunded::String
end

function Exotic()
    Exotic(Wager(),"",[],0.0,0.0,0.0,"","","")
end

mutable struct Wagers
    wps::WPS
    exotics::Array{Exotic}
end

function Wagers()
    Wagers(WPS(),[])
end

mutable struct HorseInfo
    horse_age::Int64
    horse_sex::String
    horse_sire::String
    horse_dam::String
    horse_damsire::String
end

function HorseInfo()
    HorseInfo(0,"","","","")
end

mutable struct Point
    call::String
    before::String
    position::String
    after::String
    parked::String
    lengths::String
    horse_time::String
end

function Point()
    Point("","","","","","","")
end

mutable struct FinishPoc
    call::String
    before::String
    original::String
    official::String
    parked::String
    lengths::String
    horse_time::String
    final_fraction_time::String
end

function FinishPoc()
    FinishPoc("","","","","","","","")
end

mutable struct SpeedPoc
    pace_first_fraction::Float64
    pace_second_fraction::Float64
    pace_third_fraction::Float64
    pace_final_fraction::Float64
    final_speed::Float64
end

function SpeedPoc()
    SpeedPoc(0.0,0.0,0.0,0.0,0.0)
end

mutable struct Horse
    details::Dict{String,String}
end

function Horse()
    Horse(Dict{String,String}())
end

mutable struct Driver
    driver_key::String
    driver_name::String
    details::Dict{String,String}
    OFF::Dict{String,String}
    FAST::Dict{String,String}
    AT_TRACK::Dict{String,String}
end

function Driver()
    Driver("","",Dict{String,String}(),Dict{String,String}(),Dict{String,String}(),
        Dict{String,String}())
end

mutable struct Trainer
    trainer_key::String
    trainer_name::String
    details::Dict{String,String}
end

function Trainer()
    Trainer("","",Dict{String,String}())
end

mutable struct Payout
    pos::String
    amount::Float64
end

function Payout()
    Payout("",0.0)
end

mutable struct Starter
    horse_name::String
    registration_num::String
    horse_gait::String
    race_class::String
    earnings::Float64
    horse_info::HorseInfo
    point_of_calls::Array{Point}
    finish::Array{FinishPoc}
    speed_figures::Array{SpeedPoc}
    oddstoadollar::Float64
    favorite::String
    coupled::String
    driver::Driver
    trainer::Trainer
    medication::String
    equipment::String
    race_comments::String
    claim_indicator::String
    claimprice::Float64
    program_number::String
    post::Int64
    dnum::String
    tnum::String
    m_line::String
    details::Dict{String,String}
    lines::Array{Horse}
    payouts::Array{Payout}
end

function Starter()
    Starter("","","","",0.0,HorseInfo(),[],[],[],0.0,"","",Driver(),Trainer(),
            "","","","",0.0,"",0,"","","",Dict{String,String}(),[],[])
end

mutable struct Race
    number::Int64
    index::Int64
    card_id::String
    baby::Int64
    race_type::String
    purse::Int64
    race_gait::String
    distance::Float64
    trk_cond::String
    temperature::Int64
    field_size::Int64
    race_times::Array{RaceTime}
    itv::Int64
    dtv::Int64
    all_roi::Float64
    todays_cr::Int64
    classrtg::Int64
    canadian_rate::Float64
    country::String
    off_time::String
    wagers::Wagers
    racedata::Dict{String,String}
    starters::Array{Starter}
end

function Race()
    Race(0,0,"",0,"",0,"",0.0,"",0,0,[],0,0,0.0,0,0,0.0,"","",Wagers(),Dict{String,String}(),[])
end

function raceDetails()
    (0,"-",0.0,"",0.0,"",0,0,0,0,0.0,0,0,"","F","F")
end

mutable struct Trackdata
    track::String
    send_track::String
    details::Dict{String,String}
    racedata::Array{Race}
end

@everywhere function Trackdata()
    Trackdata("","",Dict{String,String}(),[])
end

mutable struct Chart
    race_date::String
    racenum::Int64
    track::Track
    races::Array{Race}
    trackdata::Trackdata
    trainers::Dict{String,Trainer}
    drivers::Dict{String,Driver}
    dams::Dict{String,Horse}
    sires::Dict{String,Horse}
end

function Chart()
    Chart("",0,Track(),[],Trackdata(),Dict{String,Trainer}(),Dict{String,Driver}(),
        Dict{String,Horse}(),Dict{String,Horse}())
end

mutable struct horseDetails
    name::String
    horse_sex::String
    horse_sire::String
    horse_dam::String
    horse_damsire::String
    found::String
end

function horseDetails()
    horseDetails("","","","","","F")
end

mutable struct trainerDetails
    train::Dict{String, String}
end

@everywhere function trainerDetails()
    trainerDetails(Dict{String, String}())
end

mutable struct driverDetails
    OFF::Dict{String, String}
    FAST::Dict{String, String}
    AT_TRACK::Dict{String, String}
    drive::Dict{String, String}
end

@everywhere function driverDetails()
    driverDetails(Dict{String,String}(),Dict{String,String}(),Dict{String,String}(),Dict{String,String}())
end

mutable struct sireDetails
    sire::Dict{String, String}
end

@everywhere function sireDetails()
    sireDetails(Dict{String, String}())
end

mutable struct damDetails
    dam::Dict{String, String}
end

@everywhere function damDetails()
    damDetails(Dict{String, String}())
end

mutable struct l_by_date
    lines::Dict{String,Starter}
end

@everywhere function l_by_date()
    l_by_date(Dict{String, Starter}())
end

mutable struct l_by_race
    races::Dict{Int64,l_by_date}
end

@everywhere function l_by_race()
    l_by_race(Dict{Int64, l_by_date}())
end

mutable struct Lines
    tracks::Dict{String,l_by_race}
end

@everywhere function Lines()
    Lines(Dict{String,l_by_race}())
end

mutable struct Tables
    source::String
    tracks::Dict{String, Track}
    new_race::String
    horses::Dict{String, horseDetails}
    trainers::Dict{String, String}
    drivers::Dict{String, String}
    lines_by_date::Dict{Int64, Lines}
end

@everywhere function Tables()
    Tables("",Dict{String,Track}(),"N",Dict{String,horseDetails}(),
        Dict{String,String}(),Dict{String,String}(),Dict{Int64,Lines}())
end

mutable struct Stats
    starts::Int64
    wins::Int64
    places::Int64
    shows::Int64
    earnswin::Float64
    earnsplace::Float64
    earnsshow::Float64
    winpays::Float64
    placepays::Float64
    showpays::Float64
    pays::Float64
    races::Int64
    times::Float64
    rating::Float64
end

@everywhere function Stats()
    Stats(0,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0,0.0,0.0)
end

mutable struct StatsTables
    start::Int64
    current::Int64
    tracks::Dict{String, Dict{String, Dict{Int64,Stats}}}
    catagories::Dict{String, Dict{String,Stats}}
end

@everywhere function StatsTables()
    StatsTables(0,0,Dict{String,Dict{String,Dict{Int64,Stats}}}(),Dict{String,Dict{String,Stats}}())
end
