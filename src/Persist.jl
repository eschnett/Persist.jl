module Persist

# TODO: use ClusterManagers
# using ClusterManagers

# TODO: use JLD for repeatability
# using JLD

import Base: serialize, deserialize, isready, wait, fetch
export serialize, deserialize, isready, wait, fetch

export JobManager, ProcessManager, SlurmManager
export JobStatus, job_empty, job_queued, job_running, job_done, job_failed
export status, jobinfo, cancel, getstdout, getstderr, cleanup
export persist, @persist, readmgr



"""Allow only Posix fully portable filenames"""
function sanitize(str::AbstractString)
    # Only allow certain characters
    str = replace(str, r"[^-A-Za-z0-9._]", "")
    # Remove leading hyphens
    str = replace(str, r"^-*", "")
    # Disallow empty filenames
    @assert str != ""
    str
end

function quotestring(str::AbstractString)
    # Quote backslashes and quotes
    str = replace(str, r"\\", "\\\\")
    str = replace(str, r"\"", "\\\"")
    # Surround by quotes
    str = "\"$str\""
    str
end

function quotestring{S<:AbstractString}(strs::Vector{S})
    join(map(quotestring, strs), " ")
end

function rmtree(path::AbstractString)
    try
        rm(path, recursive=true)
    catch
        # We cannot remove the file or directory. This can happen for
        # several benign reasons, e.g. on NFS file systems, or if a
        # process is using it as its current directory.
        # As a work-around, create a directory "Trash" and move the
        # job directory there.
        # Note: This does not work on Windows.
        # Create trash directory
        trashdir = "Trash"
        try mkdir(trashdir) end
        # Move file or directory to trash directory
        uuid = Base.Random.uuid4()
        file = basename(path)
        newname = "$file-$uuid"
        try
            mv(path, joinpath(trashdir, newname))
        catch e
            # Ignore the error on Windows, since there doesn't seem to
            # be a work-around
            @unix_only rethrow(e)
        end
        # Try to delete trash directory, including everything that was
        # previously moved there
        try rm(trashdir, recursive=true) end
    end
    nothing
end



function jobdirname(jobname::AbstractString)
    "$(sanitize(jobname)).job"
end

function jobfilename(jobname::AbstractString)
    "$(sanitize(jobname)).bin"
end

function resultfilename(jobname::AbstractString)
    "$(sanitize(jobname)).res"
end

function outfilename(jobname::AbstractString)
    "$(sanitize(jobname)).out"
end

function errfilename(jobname::AbstractString)
    "$(sanitize(jobname)).err"
end

function mgrfilename(jobname::AbstractString)
    "$(sanitize(jobname)).mgr"
end

function shellfilename(jobname::AbstractString)
    "$(sanitize(jobname)).sh"
end



abstract JobManager

@enum JobStatus job_empty job_queued job_running job_done job_failed



function runjob(jobfile::AbstractString, resultfile::AbstractString)
    tmpfile = "$resultfile.tmp"
    try rm(resultfile) end
    try rm(tmpfile) end
    local job
    open(jobfile, "r") do f
        job = deserialize(f)
    end
    result = job()
    try
        open(tmpfile, "w") do f
            serialize(f, result)
        end
        mv(tmpfile, resultfile)
    finally
        try rm(tmpfile) end
    end
end



type ProcessManager <: JobManager
    jobname::AbstractString
    pid::Int32

    function ProcessManager(jobname::AbstractString)
        new(jobname, -1)
    end

    ProcessManager(::Base.SerializationState) = new()
end

function serialize(s::Base.SerializationState, mgr::ProcessManager)
    Base.Serializer.serialize_type(s, ProcessManager)
    serialize(s, mgr.jobname)
    serialize(s, mgr.pid)
end

function deserialize(s::Base.SerializationState, ::Type{ProcessManager})
    mgr = ProcessManager(s)
    mgr.jobname = deserialize(s)
    mgr.pid = deserialize(s)
    mgr
end

function pidfilename(jobname::AbstractString)
    "$(sanitize(jobname)).pid"
end
function donefilename(jobname::AbstractString)
    "$(sanitize(jobname)).done"
end

function submit(job, mgr::ProcessManager, nprocs::Integer)
    @assert status(mgr) == job_empty
    # Create job directory
    jobdir = jobdirname(mgr.jobname)
    try
        mkdir(jobdir)
    catch
        # There is another job with the same name
        error("Job directory \"$jobdir\"exists already")
    end
    # Serialize the Julia function
    jobfile = jobfilename(mgr.jobname)
    open(joinpath(jobdir, jobfile), "w") do f
        serialize(f, job)
    end
    # Create a wrapper script
    resultfile = resultfilename(mgr.jobname)
    outfile = outfilename(mgr.jobname)
    errfile = errfilename(mgr.jobname)
    shellfile = shellfilename(mgr.jobname)
    pidfile = pidfilename(mgr.jobname)
    donefile = donefilename(mgr.jobname)
    open(joinpath(jobdir, shellfile), "w") do f
        print(f, """
#! /bin/sh
# This is an auto-generated Julia script for the Persist package
echo \$\$ >$pidfile
$(quotestring(Base.julia_cmd().exec)) -p $nprocs -e 'using Persist; Persist.runjob($(quotestring(jobfile)), $(quotestring(resultfile)))' </dev/null >$outfile 2>$errfile
: >$donefile
""")
    end
    # Pre-create output files
    open(joinpath(jobdir, outfile), "w") do f end
    open(joinpath(jobdir, errfile), "w") do f end
    open(joinpath(jobdir, pidfile), "w") do f end
    # Start the job in the job directory
    spawn(detach(setenv(`sh $shellfile`, dir=jobdir)))
    # Wait for the job to output its pid
    # TODO: We should get the pid from spwan, but I don't know how
    local buf
    while true
        buf = readall(joinpath(jobdir, pidfile))
        if endswith(buf, '\n') break end
        sleep(0.1)
    end
    mgr.pid = parse(Int, buf)
    info("Process id: $(mgr.pid)")
    # Serialize the manager
    mgrfile = mgrfilename(mgr.jobname)
    open(joinpath(jobdir, mgrfile), "w") do f
        serialize(f, mgr)
    end
    nothing
end

function status(mgr::ProcessManager)
    if mgr.pid < 0 return job_empty end
    # It seems that we can't check the process pid, since the process will
    # live too long -- this is probably a problem in detach
    # if success(pipeline(`ps -p $(mgr.pid)`, stdout=DevNull, stderr=DevNull))
    #     job_running
    # else
    #     job_done
    # end
    donefile = joinpath(jobdirname(mgr.jobname), donefilename(mgr.jobname))
    try
        open(donefile, "r") do f end
        resultfile =
            joinpath(jobdirname(mgr.jobname), resultfilename(mgr.jobname))
        isfile(resultfile) && return job_done
        return job_failed
    end
    job_running
end

function jobinfo(mgr::ProcessManager)
    st = status(mgr)
    @assert st != job_empty
    if st == job_queued return "[job_queued]" end
    if st == job_running
        try
            return readall(`ps -f -p $(mgr.pid)`)
        end
        # `ps` failed; most likely because the process does not exist any more
    end
    "[job_done]"
end

function cancel(mgr::ProcessManager; force::Bool=false)
    @assert status(mgr) != job_empty
    signum = force ? "SIGKILL" : "SIGTERM"
    run(pipeline(ignorestatus(`kill -$signum $(mgr.pid)`),
                 stdout=DevNull, stderr=DevNull))
    # Remove the pid file since the job won't do it any more
    # TODO: The job may still be running, and we will never know.
    donefile = joinpath(jobdirname(mgr.jobname), donefilename(mgr.jobname))
    open(donefile, "w") do f end
    nothing
end

function isready(mgr::ProcessManager)
    return status(mgr) == job_done
end

function wait(mgr::ProcessManager)
    @assert status(mgr) != job_empty
    while status(mgr) != job_done
        sleep(1)
    end
    nothing
end

function fetch(mgr::ProcessManager)
    @assert status(mgr) != job_empty
    wait(mgr)
    resultname = joinpath(jobdirname(mgr.jobname), resultfilename(mgr.jobname))
    local result
    open(resultname, "r") do f
        result = deserialize(f)
    end
    result
end

function getstdout(mgr::ProcessManager)
    @assert status(mgr) != job_empty
    readall(joinpath(jobdirname(mgr.jobname), outfilename(mgr.jobname)))
end

function getstderr(mgr::ProcessManager)
    @assert status(mgr) != job_empty
    readall(joinpath(jobdirname(mgr.jobname), errfilename(mgr.jobname)))
end

function cleanup(mgr::ProcessManager)
    @assert status(mgr) in (job_done, job_failed)
    rmtree(jobdirname(mgr.jobname))
    mgr.pid = -1
    nothing
end



type SlurmManager <: JobManager
    jobname::AbstractString
    jobid::AbstractString

    function SlurmManager(jobname::AbstractString)
        new(jobname, "")
    end

    SlurmManager(::Base.SerializationState) = new()
end

function serialize(s::Base.SerializationState, mgr::SlurmManager)
    Base.Serializer.serialize_type(s, SlurmManager)
    serialize(s, mgr.jobname)
    serialize(s, mgr.jobid)
end

function deserialize(s::Base.SerializationState, ::Type{SlurmManager})
    mgr = SlurmManager(s)
    mgr.jobname = deserialize(s)
    mgr.jobid = deserialize(s)
    mgr
end

function submit(job, mgr::SlurmManager, nprocs::Integer)
    @assert status(mgr) == job_empty
    # Create job directory
    jobdir = jobdirname(mgr.jobname)
    jobdir = abspath(jobdir)
    try
        mkdir(jobdir)
    catch
        # There is another job with the same name
        error("Job directory \"$jobdir\"exists already")
    end
    # Serialize the Julia function
    jobfile = jobfilename(mgr.jobname)
    open(joinpath(jobdir, jobfile), "w") do f
        serialize(f, job)
    end
    # Create a wrapper script
    resultfile = resultfilename(mgr.jobname)
    outfile = outfilename(mgr.jobname)
    errfile = errfilename(mgr.jobname)
    shellfile = shellfilename(mgr.jobname)
    open(joinpath(jobdir, shellfile), "w") do f
        print(f, """
#! /bin/sh
# This is an auto-generated Julia script for the Persist package
hostname
$(quotestring(Base.julia_cmd().exec)) -p $nprocs -e 'using Persist; Persist.runjob($(quotestring(jobfile)), $(quotestring(resultfile)))' </dev/null >$outfile 2>$errfile
""")
    end
    # Pre-create output files
    open(joinpath(jobdir, outfile), "w") do f end
    open(joinpath(jobdir, errfile), "w") do f end
    # Start the job in the job directory
    # TODO: Teach Julia how to use the nodes that Slurm reserved
    buf = readall(setenv(`sbatch -D $jobdir -J $(mgr.jobname) -n $nprocs $shellfile`,
                         dir=jobdir))
    m = match(r"Submitted batch job ([0-9]+)", buf)
    mgr.jobid = m.captures[1]
    info("Slurm job id: $(mgr.jobid)")
    # Serialize the manager
    mgrfile = mgrfilename(mgr.jobname)
    open(joinpath(jobdir, mgrfile), "w") do f
        serialize(f, mgr)
    end
    nothing
end

function status(mgr::SlurmManager)
    if isempty(mgr.jobid) return job_empty end
    try
        buf = readall(`squeue -h -j $(mgr.jobid) -o '%t'`)
        state = chomp(buf)
        if state in ["CF", "PD"]
            return job_queued
        elseif state in ["CG", "R", "S"]
            return jo_running
        elseif state in ["CA", "CD", "F", "NF", "PR", "TO"]
            return job_done
        else
            @assert false
        end
    end
    # Slurm knows nothing about this job
    resultfile = joinpath(jobdirname(mgr.jobname), resultfilename(mgr.jobname))
    isfile(resultfile) && job_done
    job_failed
end

function jobinfo(mgr::SlurmManager)
    st = status(mgr)
    @assert st != job_empty
    try
        return readall(`squeue -j $(mgr.jobid)`)
    end
    # Slurm knows nothing about this job
    "[job_done]"
end

function cancel(mgr::SlurmManager; force::Bool=false)
    @assert status(mgr) != job_empty
    # TODO: Handle things differently for force=false and force=true
    run(`scancel -j $(mgr.jobid)`)
    nothing
end

function isready(mgr::SlurmManager)
    return status(mgr) == job_done
end

function wait(mgr::SlurmManager)
    @assert status(mgr) != job_empty
    while status(mgr) != job_done
        sleep(1)
    end
    nothing
end

function fetch(mgr::SlurmManager)
    wait(mgr)
    resultname = joinpath(jobdirname(mgr.jobname), resultfilename(mgr.jobname))
    local result
    open(resultname, "r") do f
        result = deserialize(f)
    end
    result
end

function getstdout(mgr::SlurmManager)
    @assert status(mgr) != job_empty
    # TODO: Read stdout while job is running
    readall(joinpath(jobdirname(mgr.jobname), outfilename(mgr.jobname)))
end

function getstderr(mgr::SlurmManager)
    @assert status(mgr) != job_empty
    # TODO: Read stdout while job is running
    readall(joinpath(jobdirname(mgr.jobname), errfilename(mgr.jobname)))
end

function cleanup(mgr::SlurmManager)
    @assert status(mgr) in (job_done, job_failed)
    rmtree(jobdirname(mgr.jobname))
    mgr.jobid = ""
    nothing
end



function persist{JM<:JobManager}(job, jobname::AbstractString, ::Type{JM},
                                 nprocs::Integer)
    mgr = JM(jobname)
    submit(job, mgr, nprocs)
    mgr::JM
end

macro persist(jobname, mgrtype, nprocs, expr)
    expr = Base.localize_vars(:(()->$expr), false)
    :(persist($(esc(expr)), $(esc(jobname)), $(esc(mgrtype)), $(esc(nprocs))))
end

function readmgr(jobname::AbstractString)
    mgrfile = joinpath(jobdirname(jobname), mgrfilename(jobname))
    local mgr
    open(mgrfile, "r") do f
        mgr = deserialize(f)
    end
    mgr::JobManager
end

end # module
