# vst-prod
VST prod code

TO RUN LOCALLY
1. Use Julia version 0.7
2. Open /config.jl and set local directories
3. Copy trackmaster data to local data directory pps directory


Run Order
1. /TrackMaster.jl -- extracts relevant PastPerformance and Results
2. /build_races.jl -- creates a racelines table/file .csv 
3. /build_stats.jl  -- creates intermediate statistics for Driver, Tranier, Horse, Track etc.
4. /build_fast_forms.jl -- produces the large number of factors.
5. /compile_forms.jl -- this is the large dataframe used for experimentation.
