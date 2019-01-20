using Base.Dates

@everywhere function parse_date(ds)
	clean = strip(replace(ds, r"(AM|PM)", ""))
	date  = DateTime(clean, DateFormat("m/d/y H:M:S"))
	if contains(ds, "PM") && hour(date) != 12
		date += Hour(12)
	elseif contains(ds, "AM") && hour(date) == 12
		date -= Hour(12)
	end
	date
end

@everywhere function invalid_track(track, races::Library)
    !haskey(races.Cards, track)
end
