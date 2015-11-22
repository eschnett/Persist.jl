module Persist

# TODO: use ClusterManagers
# using ClusterManagers

# TODO: use JLD for repeatability
# using JLD

import Base: serialize, deserialize
export serialize, deserialize

export JobManager, ProcessManager, SlurmManager
export status, jobinfo, cancel, waitjob, getstdout, getstderr, cleanup
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

function jobdirname(jobname::AbstractString)
    "$(sanitize(jobname)).job"
end

function jobfilename(jobname::AbstractString)
    "$(sanitize(jobname)).bin"
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



function runjob(jobfile::AbstractString)
    local job
    open(jobfile, "r") do f
        job = deserialize(f)
    end
    job()
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
    @assert status(mgr) == :empty
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
julia -p $nprocs -e 'using Persist; Persist.runjob($(quotestring(jobfile)))' </dev/null >$outfile 2>$errfile
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

"""status(mgr::JobManager) returns either :empty, :queued, :running, or :done"""
function status(mgr::ProcessManager)
    if mgr.pid < 0 return :empty end
    # It seems that we can't check the process pid, since the process will
    # live too long -- this is probably a problem in detach
    # if success(pipeline(`ps -p $(mgr.pid)`, stdout=DevNull, stderr=DevNull))
    #     :running
    # else
    #     :done
    # end
    donefile = joinpath(jobdirname(mgr.jobname), donefilename(mgr.jobname))
    try
        open(donefile, "r") do f end
        return :done
    end
    :running
end

function jobinfo(mgr::ProcessManager)
    st = status(mgr)
    @assert st != :empty
    if st == :queued return "[:queued]" end
    if st == :running
        try
            return readall(`ps -f -p $(mgr.pid)`)
        end
        # `ps` failed; most likely because the process does not exist any more
    end
    "[:done]"
end

function cancel(mgr::ProcessManager; force::Bool=false)
    @assert status(mgr) != :empty
    signum = force ? "SIGKILL" : "SIGTERM"
    run(pipeline(ignorestatus(`kill -$signum $(mgr.pid)`),
                 stdout=DevNull, stderr=DevNull))
    # Remove the pid file since the job won't do it any more
    # TODO: The job may still be running, and we will never know.
    donefile = joinpath(jobdirname(mgr.jobname), donefilename(mgr.jobname))
    open(donefile, "w") do f end
    nothing
end

function waitjob(mgr::ProcessManager)
    @assert status(mgr) != :empty
    while status(mgr) == :running
        sleep(1)
    end
    nothing
end

function getstdout(mgr::ProcessManager)
    @assert status(mgr) != :empty
    readall(joinpath(jobdirname(mgr.jobname), outfilename(mgr.jobname)))
end

function getstderr(mgr::ProcessManager)
    @assert status(mgr) != :empty
    readall(joinpath(jobdirname(mgr.jobname), errfilename(mgr.jobname)))
end

function cleanup(mgr::ProcessManager)
    info("cleanup.0")
    @assert status(mgr) == :done
    info("cleanup.1")
    try
        info("cleanup.2")
        rm(jobdirname(mgr.jobname), recursive=true)
        info("cleanup.3")
    catch
        info("cleanup.4")
        # We cannot remove the job directory. This can happen for
        # several benign reasons, e.g. on NFS file systems, or if the
        # subprocess has not yet been cleaned up. We create a
        # directory "Trash" and move the job directory there.
        # Create trash directory
        trashdir = "Trash"
        try mkdir(trashdir) end
        info("cleanup.5")
        # Move job directory to trash directory
        uuid = Base.Random.uuid4()
        newname = "$(jobdirname(mgr.jobname))-$uuid"
        info("cleanup.6")
        mv(jobdirname(mgr.jobname), joinpath(trashdir, newname))
        info("cleanup.7")
        # Try to delete trash directory, including everything that was
        # previously move there
        try rm(trashdir, recursive=true) end
        info("cleanup.8")
    end
    info("cleanup.9")
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
    @assert status(mgr) == :empty
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
    local exe
    try
        exe = abspath(joinpath(JULIA_HOME, "julia"))
    catch
        exe = "julia"
    end
    outfile = outfilename(mgr.jobname)
    errfile = errfilename(mgr.jobname)
    shellfile = shellfilename(mgr.jobname)
    open(joinpath(jobdir, shellfile), "w") do f
        print(f, """
#! /bin/sh
# This is an auto-generated Julia script for the Persist package
hostname
$exe -p $nprocs -e 'using Persist; Persist.runjob($(quotestring(jobfile)))' </dev/null >$outfile 2>$errfile
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

"""status(mgr::JobManager) returns either :empty, :queued, :running, or :done"""
function status(mgr::SlurmManager)
    if isempty(mgr.jobid) return :empty end
    try
        buf = readall(`squeue -h -j $(mgr.jobid) -o '%t'`)
        state = chomp(buf)
        if state in ["CF", "PD"]
            return :queued
        elseif state in ["CG", "R", "S"]
            return :running
        elseif state in ["CA", "CD", "F", "NF", "PR", "TO"]
            return :done
        else
            @assert false
        end
    end
    # Slurm knows nothing about this job
    :done
end

function jobinfo(mgr::SlurmManager)
    st = status(mgr)
    @assert st != :empty
    try
        return readall(`squeue -j $(mgr.jobid)`)
    end
    # Slurm knows nothing about this job
    "[:done]"
end

function cancel(mgr::SlurmManager; force::Bool=false)
    @assert status(mgr) != :empty
    # TODO: Handle things differently for force=false and force=true
    run(`scancel -j $(mgr.jobid)`)
    nothing
end

function waitjob(mgr::SlurmManager)
    @assert status(mgr) != :empty
    while status(mgr) != :done
        sleep(1)
    end
    nothing
end

function getstdout(mgr::SlurmManager)
    @assert status(mgr) != :empty
    # TODO: Read stdout while job is running
    readall(joinpath(jobdirname(mgr.jobname), outfilename(mgr.jobname)))
end

function getstderr(mgr::SlurmManager)
    @assert status(mgr) != :empty
    # TODO: Read stdout while job is running
    readall(joinpath(jobdirname(mgr.jobname), errfilename(mgr.jobname)))
end

function cleanup(mgr::SlurmManager)
    @assert status(mgr) == :done
    try
        rm(jobdirname(mgr.jobname), recursive=true)
    catch
        # We cannot remove the job directory. This can happen for
        # several benign reasons, e.g. on NFS file systems, or if the
        # subprocess has not yet been cleaned up. We create a
        # directory "Trash" and move the job directory there.
        # Create trash directory
        trashdir = "Trash"
        try mkdir(trashdir) end
        # Move job directory to trash directory
        uuid = Base.Random.uuid4()
        newname = "$(jobdirname(mgr.jobname))-$uuid"
        mv(jobdirname(mgr.jobname), joinpath(trashdir, newname))
        # Try to delete trash directory, including everything that was
        # previously move there
        try rm(trashdir, recursive=true) end
    end
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
