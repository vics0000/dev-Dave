# This file handles parsing through an AMT log file. It will iterate through each record, decode the metadata, and then
# pass the XML (if any) to the parser in amt_xml.jl

#include("/home/whatwins/www/horses/models.jl")
#@everywhere include("amt_type.jl")
#@everywhere include("amt_xml.jl")

# Parse a "live" record which does not have the same header prefix as the log record
@everywhere function amt_parse_live!(rec, track, races)
    # Create a callback data object, parse the xml record, then do the callback
    data = AmtCallbackData()
    data.track = track
    data.date = ceil(DateTime(now()),Dates.Second)
    if (rec[1] == '<')
		# They say utf-16, but don't really mean it. It's breaking the parser!
		rec = replace(rec, "utf-16", "utf-8")
        data.obj = amt_parse_xml(rec)
    else
        data.obj = rec
    end
    cb_amt_obj!(data, races)
end

# Parse one record of the AMT log file
@everywhere function amt_parse_record!(rec, ln, races)
    rec = strip(rec)
    if isempty(rec) return end
    ts = searchindex(rec, ":TRACE")
    dt = parse_date(rec[1:ts-1])
    ds = ts + 15 # data start
    # If the next character is a colon, then skip over it.
    # Otherwise we have a process directive
    if (rec[ds] == ':')
        ds = ds + 1
        proc = ""
    else
        c = searchindex(rec, ':', ds+1)
        proc = rec[ds:c-1]
        ds = c+1
    end
    # If processInit, then nothing else to do
    if (proc == "processInit")
        return
    end
    # Get the XML block and args
    x = searchindex(rec, "<?xml")
    if (x > 0)
        args = split(rec[ds:x-1], ';')
    else
        args = split(rec[ds:end], ';')
    end
    rtype = shift!(args)
    if (length(args) > 1)
        feed = shift!(args)
        track = shift!(args)
    else
        feed = "";
        track = "";
    end
    # Create a callback data object, parse the xml record, then do the callback
    data = AmtCallbackData()
    data.track = track
    data.ln = ln
    data.date = dt
    if (rtype != "PROGRAMLIST")&&(invalid_track(track, races)&&(!isempty(races.Cards)))
        if dt < (races.ticker + Dates.Second(5)) return end
        cb_amt_obj!(data, races)
        return
    end
    if (rtype == "MTP")
        data.obj = UpdateRaceTimes();
        data.obj.programName = track
        data.obj.race = parse(Int64, shift!(args))
        data.obj.mtp = parse(Int64, shift!(args))
        data.obj.postTime = shift!(args)
    elseif (rtype == "CYCLE" ||
        rtype == "ITP" ||
        rtype == "RACEDEF")
        data.obj = amt_parse_xml(rec[x:end])
    elseif (rtype == "PROGRAMLIST" ||
        rtype == "PROGRAMDEF" ||
        rtype == "POOLSPELLING" ||
        rtype == "POOLHOLDER" ||
        rtype == "WILLPAY" ||
        rtype == "PRICES")
        data.obj = amt_parse_xml(rec[x:end])
    elseif (rtype == "START")
        data.obj = NotifyStartBetting();
        data.obj.programName = track
        data.obj.race = parse(Int64, shift!(args))
        data.obj.time = shift!(args)
    elseif (rtype == "STOP")
        data.obj = NotifyStopBetting();
        data.obj.programName = track
        data.obj.race = parse(Int64, shift!(args))
    elseif (rtype == "ChangeData")
        data.obj = NotifyChangeData();
        data.obj.programName = track
        data.obj.race = parse(Int64, shift!(args))
        data.obj.changeType = shift!(args)
        data.obj.changeData = shift!(args)
        data.obj.timeSent = shift!(args)
    elseif (rtype == "ENTRYRUNNER")
        data.obj = NotifyEntryRunnerStatus();
        data.obj.programName = track
        data.obj.race = parse(Int64, shift!(args))
        data.obj.runner = shift!(args)
        data.obj.status = shift!(args)
    elseif (rtype == "RUNNER")
        data.obj = NotifyRunnerStatus();
        data.obj.programName = track
        data.obj.race = parse(Int64, shift!(args))
        data.obj.runner = shift!(args)
        data.obj.status = shift!(args)
    elseif (rtype == "CARRYIN")
        data.obj = NotifyPoolCarryIn();
        data.obj.programName = track
        data.obj.race = parse(Int64, shift!(args))
        data.obj.poolCarryIn = shift!(args)
    end
    cb_amt_obj!(data, races)
end

# Process an entire AMT log file. If a .zip file is provided, then it will be unzipped in the current directory and removed afterwards.
@everywhere function amt_process_file!(logpath, races)
    cleanup = false
    if (ismatch(r"\.zip$", logpath))
        println("Its a zip file: $logpath")
        run(`unzip $logpath`)
        parts = split(logpath, '/')
        fn = pop!(parts)
        logpath = replace(fn, r"\.zip$", ".log")
        cleanup = true
    end
    if (isfile(logpath))
        fp = open(logpath, "r")
        ln = 1
        for txt in eachline(fp)
            amt_parse_record!(txt, ln, races)
            ln += 1
        end
        close(fp)
    else
        println("File does not exist: $logpath")
        return
    end
    if (cleanup)
        rm(logpath)
    end
end
