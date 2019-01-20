# This file defines all the data types that can be parsed from the XML file and those needed to manage races
using DataArrays, DataFrames
using JLD, CSV

# For models where the weights
type CompositeLinearModelA
    submodels::Vector{Any}
    weights::Vector{Any}
end

type ExactaModel
    h1_model::Any
    h2_model::Any
end

type TrifectaModel
    h1_model::Any
    h2_model::Any
    h3_model::Any
end

type AmtCallbackData
    track::String
    ln::Int64
    date::Any
    obj
end

@everywhere function AmtCallbackData()
    AmtCallbackData("", 0, nothing, nothing)
end

type priceDB
    Rnum::Int64
    Date::Int64
    Source::String
    poolID::String
    baseValue::Float64
    name::String
    results::String
    reasonX::String
    reasonY::String
    reasonZ::String
    paid::Float64
end

@everywhere function priceDB()
    priceDB(0,0,"","",0.0,"","","","","",0.0)
end

type OddsDB
    Rnum::Int64
    Date::Int64
    Source::String
    Kind::String
    Timestamp::String
    Exotic::String
    First::Int64
    Second::Int64
    Value::Float64
end

@everywhere function OddsDB()
    OddsDB(0,0,"","","","",0,0,0.0)
end

type wagerDB
    Rnum::Int64
    Date::Int64
    ModelVal::Float64
    ModelTag::String
    ModelHash::String
    Track::String
    Race::Int64
    Source::String
    Exotic::String
    First::Int64
    Second::Int64
    Third::Int64
    Fourth::Int64
    Fifth::Int64
    Tickets::Int64
    Wager::Float64
    Balance::Float64
    aWager::Float64
    Horses::Int64
end

@everywhere function wagerDB()
    wagerDB(0,0,0,"","","",0,"","",0,0,0,0,0,0,0,0,0,0)
end

type BetTypeDefinition
    betTypeName::String
    validPermutations::String
end

@everywhere function BetTypeDefinition()
    BetTypeDefinition("","")
end

type BetTypeInformation
    raceNumber::Int64
    betTypeName::String
    validPermutations::String
    numberOfAdditionalLegs::Int64
    legList::Array{Int64}
    minBaseAmount::Float64
    maxBaseAmount::Float64
    minBoxAmount::Float64
    minWheelAmount::Float64
    maxPermuteAmount::Float64
    multipleAmount::Float64
end

@everywhere function BetTypeInformation()
    BetTypeInformation(0,"","",0,[],0,0,0,0,0,0)
end

type ProgramInfo
    off::Int64
    delay::Int64
    ticker::Int64
    pt::DateTime
    posted::Int64
    programNumber::Int64
    programName::String
    programDate::Int64
    programStatus::String
    programLongName::String
    maxRunners::Int64
    highestRace::Int64
    currentRace::Int64
    minutesToPost::Int64
    trackState::String
    trackZipCode::String
    displayName::String
    countryCode::String
    numberOfRaces::Int64
    numberOfBetTypes::Int64
end

@everywhere function ProgramInfo()
    ProgramInfo(0,0,9999,DateTime(),0,0,"",0,"","",0,0,0,0,"","","","",0,0)
end

type GpAvailablePrograms
    numberOfPrograms::Int64
    programList::Array{ProgramInfo}
end

@everywhere function GpAvailablePrograms()
    GpAvailablePrograms(0, [])
end

type GpOdds
    programName::String
    race::Int64
    betType::String
    source::Int64
    cycleType::String
    nrRows::Int64
    nrValuesPerRow::Int64
    odds::Array{String}
end

@everywhere function GpOdds()
    GpOdds("", 0, "", 0, "", 0, 0, [])
end

type GpProbs
    programName::String
    race::Int64
    betType::String
    source::Int64
    cycleType::String
    baseAmount::String # Usually int, but have seen "RF"
    nrRows::Int64
    nrValuesPerRow::Int64
    probs::Array{String}
end

@everywhere function GpProbs()
    GpProbs("", 0, "", 0, "", "", 0, 0, [])
end

type PriceRecord
    poolID::String
    baseValue::Float64
    name::String
    results::String
    reasonX::String
    reasonY::String
    reasonZ::String
    paid::Float64
end

@everywhere function PriceRecord()
    PriceRecord("",0,"","","","","",0.0)
end

type PriceInfo
    scratches::String
    raceTime::String
    toteFavorite::String
end

@everywhere function PriceInfo()
    PriceInfo("","","")
end

type GpPrices
    programName::String
    race::Int64
    source::Int64
    orderOfFinish::String
    priceInfo::PriceInfo
    prices::Array{PriceRecord}
    date::String
end

@everywhere function GpPrices()
    GpPrices("",0,0,"",PriceInfo(),[],"")
end

type GpPool
    programName::String
    race::Int64
    betType::String
    source::Int64
    cycleType::String
    poolTotal
    poolChange::Float64
    nrRows::Int64
    nrValuesPerRow::Int64
    money::Array{String}
end

@everywhere function GpPool()
    GpPool("", 0, "", 0, "", 0, 0.0, 0, 0, [])
end

type GpCycleData
    programName::String
    race::Int64
    source::Int64 # TODO: check this type against the spec
    odds::Array{GpOdds}
    probs::Array{GpProbs}
    pools::Array{GpPool}
end

@everywhere function GpCycleData()
    GpCycleData("", 0, 0, [], [], [])
end

type TrackRecord
    raceDate::Int64
    horseName::String
    horseAge::Int64
    breedType::String
    sex::String
    winningTime::Int64
    weightCarried::Int64
end

@everywhere function TrackRecord()
    TrackRecord(0,"",0,"","",0,0)
end

type Track
    trackID::String
    trackName::String
    country::String
    timeZone::String
end

@everywhere function Track()
    Track("","","","")
end

type Race
    raceDate::Int64
    dayEvening::String
    raceNumber::Int64
    breedType::String
    raceName::String
    raceType::String
    conditions::String
    distance::String
    purseUSA::Float64
    surface::String
    postTime::String
    wagerText::String
    ageRestriction::String
    sexRestriction::String
    grade::String
    division::String
    gait::String
    maximumClaimingPriceUSA::String
    minimumClaimingPriceUSA::String
    programSelections::String
    trackRecord::Array{TrackRecord}
end

@everywhere function Race()
    Race(0,"",0,"","","","","",0.0,"","","","","","","","","","","",[])
end

type RaceSummary
    year::Int64
    surface::String
    numberOfStarts::Int64
    numberOfWins::Int64
    numberOfSeconds::Int64
    numberOfThirds::Int64
    earningUSA::Float64
end

@everywhere function RaceSummary()
    RaceSummary(0,"",0,0,0,0,0.0)
end

type Selection
    hnum::Int64
    dnum::Int64
    tnum::Int64
    name::String
    yearOfBirth::Int64
    foalingArea::String
    breedType::String
    color::String
    sex::String
    horseDamName::String
    horseSireName::String
    breederName::String
    trainerName::String
    ownerName::String
    racingOwnerSilks::String
    jockeyName::String
    postPosition::Int64
    programNumber::String
    weightCarried::String
    medication::String
    equipment::String
    morningLine::String
    claimedPriceUSA::Float64
    todaysHorseClassRating::String
    saddleClothColor::String
    raceSummary::Array{RaceSummary}
end

@everywhere function Selection()
    Selection(0,0,0,"",0,"","","","","","","","","","","",0,"","","","","",0.0,"","",[])
end

type Starters
    a::Int64
end

@everywhere function Starters()
    Starters(0)
end

type ItpData
    track::Track
    race::Race
    starters::Array{Selection}
end

@everywhere function ItpData()
    ItpData(Track(), Race(), [])
end

type GpItpData
    itpData::ItpData
end

@everywhere function GpItpData()
    GpItpData(ItpData())
end

type LateResult
    result::String
    reason::String
    theValue::String
end

@everywhere function LateResult()
    LateResult("","","")
end

type WillPays
    programName::String
    race::Int64
    betType::String
    source::Int64
    baseAmount::String
    earlyResult::String
    willPayList::Array{LateResult}
end

@everywhere function WillPays()
    WillPays("",0,"",0,"","",[])
end

type RaceDefinition
    rnum::Int64
    posted::Int64
    off::Int64
    form::DataFrame
    raceNumber::Int64
    featureNumber::Int64
    liveRunners::Array{Int64}
    scratchedRunners::Array{Int64}
    openBetTypes::Array{String}
    betTypeInformation::Array{BetTypeInformation}
    ItpData::ItpData
    oldCycleData::GpCycleData
    savedCycleData::GpCycleData
    CycleData::GpCycleData
    willPaysList::Dict{String,WillPays}
    wagers::DataFrame
    odds::DataFrame
end

@everywhere function RaceDefinition()
    RaceDefinition(0,0,0,DataFrame(),0,0,[],[],[],[],ItpData(),GpCycleData(),GpCycleData(),GpCycleData(),
                   Dict{String,WillPays}(),DataFrame(),DataFrame())
end

type GpRaceDefinition
    programName::String
    raceDetails::RaceDefinition
end

@everywhere function GpRaceDefinition()
    GpRaceDefinition("", RaceDefinition())
end

type NotifyStartBetting
    programName::String
    race::Int64
    time::String
end

@everywhere function NotifyStartBetting()
    NotifyStartBetting("",0,"")
end

type NotifyStopBetting
    programName::String
    race::Int64
end

@everywhere function NotifyStopBetting()
    NotifyStopBetting("",0)
end

type NotifyChangeData
    programName::String
    race::Int64
    changeType::String
    changeData::String
    timeSent::String
end

@everywhere function NotifyChangeData()
    NotifyChangeData("",0,"","","")
end

type NotifyEntryRunnerStatus
    programName::String
    race::Int64
    runner::String
    status::String
end

@everywhere function NotifyEntryRunnerStatus()
    NotifyEntryRunnerStatus("",0,"","")
end

type NotifyPoolCarryIn
    programName::String
    race::Int64
    poolCarryIn::String
end

@everywhere function NotifyPoolCarryIn()
    NotifyPoolCarryIn("",0,"")
end

type NotifyRunnerStatus
    programName::String
    race::Int64
    runner::String
    status::String
end

@everywhere function NotifyRunnerStatus()
    NotifyRunnerStatus("",0,"","")
end

type UpdateRaceTimes
    programName::String
    race::Int64
    mtp::Int64
    postTime::String
end

@everywhere function UpdateRaceTimes()
    UpdateRaceTimes("",0,0,"")
end

type GpProgramDefinition
    programName::String
    numberOfBetTypes::Int64
    betTypeList::Array{BetTypeDefinition}
    numberOfRaces::Int64
    raceDetailList::Dict{Int64, RaceDefinition}
end

@everywhere function GpProgramDefinition()
    GpProgramDefinition("",0,[],0,Dict{Int64, RaceDefinition}())
end

type GpProgramDetail
    programNumber::Int64
    programName::String
    programLongName::String
    programDate::String
    programITWDate::String
    maxRunners::Int64
    programType::String
    programState::String
    zipCodeOfTrack::String
    alternateDisplayName::String
    countryCode::String
    chan::Int64
end

@everywhere function GpProgramDetail()
    GpProgramDetail(0,"","","","",0,"","","","","",0)
end

type GpProgramCombine
    detail::GpProgramDetail
    definition::GpProgramDefinition
end

@everywhere function GpProgramCombine()
    GpProgramCombine(GpProgramDetail(),GpProgramDefinition())
end

type PoolHolder
    poolCode::String
    contributorType::String
end

@everywhere function PoolHolder()
    PoolHolder("","")
end

type GpPoolHolder
    programName::String
    race::Int64
    poolHolders::Array{PoolHolder}
    source::Int64
end

@everywhere function GpPoolHolder()
    GpPoolHolder("",0,[],0)
end

type RaceData
    Track::String
    CardInfo::ProgramInfo
    Races::Dict{Int64, RaceDefinition}
end

@everywhere function RaceData()
    RaceData("", ProgramInfo(), Dict{Int64,RaceDefinition}())
end

type eqnDefn
    func::String
    tt::String
    kind::String
    Intercept::Float64
    Coefficient::Float64
    Cutoff::Float64
    eqn::DataFrame
    betIndexes::DataFrame
    model
end

@everywhere function eqnDefn()
    eqnDefn("","","",0,0,0,DataFrame(),DataFrame(),[])
end

type gaitDefn
    gait::Dict{String, eqnDefn}
end

@everywhere function gaitDefn()
    gaitDefn(Dict{String,eqnDefn}())
end

type betDefn
    gaits::Dict{String, gaitDefn}
end

@everywhere function betDefn()
    betDefn(Dict{String,gaitDefn}())
end

type Library
    source::String
    inExt::String
    outExt::String
    sock::TCPSocket
    firstPost::DateTime
    firstRun::Bool
    ticker::DateTime
    changes::Int64
    Rebates::DataFrame
    delays::DataFrame
    forms::DataFrame
    best::DataFrame
    betEqns::Dict{String, betDefn}
    Cards::Dict{String, RaceData}
end

@everywhere function Library()
    Library("","","",TCPSocket(),DateTime(),true,DateTime(),0,DataFrame(),
            DataFrame(),DataFrame(),DataFrame(),Dict{String,betDefn}(),Dict{String,RaceData}())
end
