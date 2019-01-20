using DataArrays, DataFrames

@everywhere function get_daily_input(date)
	dir = ENV["DATADIR"]*"dailies/$date/"
	if !isfile(dir * "Delays.csv") cp(ENV["DATADIR"]*"Delays.csv", dir*"Delays.csv") end
    delays = CSV.read(dir*"Delays.csv")
    if !isfile(dir*"FormsQUA.csv") return false, false end
    forms = CSV.read(dir*"FormsQUA.csv")
	forms, delays
end
