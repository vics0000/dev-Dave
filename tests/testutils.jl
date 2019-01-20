# testutils.jl

# remove run-data
# mkdir run-data
# copy from setup-data to run-data
# ignore in ignorefiles array like ".DS_Store"
function setupData(testdir::String)
    clearRunData(testdir)
    copySetupDataToRunData(testdir)
end

function clearRunData(testdir::String)
    rundir = testdir*"/run-data"
    rm(rundir, force=true, recursive=true)
    mkdir(rundir)
end

function copySetupDataToRunData(testdir::String)
    ignoreFiles = [".DS_Store"]
    setupdir = testdir*"/setup-data/"
    rundir = testdir*"/run-data/"

    for f in readdir(setupdir)
        if findfirst(ignoreFiles, f) > 0
            continue
        end
        cp(setupdir*f, rundir*f)
    end


    # for (root, dirs, files) in walkdir(dir)
    #     println("Directories in $root")
    #     for dir in dirs
    #         println(joinpath(root, dir)) # path to directories
    #     end
    #     println("Files in $root")
    #     for file in files
    #         println(joinpath(root, file)) # path to files
    #     end
    # end
end
