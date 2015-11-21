module Persist

# TODO: use JLD for repeatability
using JLD

import Base: serialize, deserialize
export serialize, deserialize

export JobManager, ProcessManager
export status, jobinfo, cancel, waitjob, getstdout, getstderr, cleanup
export persist, readmgr



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
  # TODO: report deserialize(s, AbstractString)
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
  # Serialize the manager
  mgrfile = mgrfilename(mgr.jobname)
  open(joinpath(jobdir, mgrfile), "w") do f
    serialize(f, mgr)
  end
end

"""status(mgr::JobManager) returns either :empty, :queued, :running, or :done"""
function status(mgr::ProcessManager)
  if mgr.pid < 0 return :empty end
  # It seems that we can't check the process pid, since the process will
  # live too long -- this is probably a problem in detach
  # if success(pipeline(`ps -p $(mgr.pid)`, stdout=DevNull, stderr=DevNull))
  #   :running
  # else
  #   :done
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
end

function waitjob(mgr::ProcessManager)
  @assert status(mgr) != :empty
  while status(mgr) == :running
    sleep(1)
  end
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
  @assert status(mgr) == :done
  try rm(jobdirname(mgr.jobname), recursive=true) end
  mgr.pid = -1
end



function persist{JM<:JobManager}(job, jobname::AbstractString, ::Type{JM},
                                 nprocs::Integer)
  mgr = JM(jobname)
  submit(job, mgr, nprocs)
  mgr::JM
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
