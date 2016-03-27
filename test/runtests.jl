using Persist
using Base.Test
using Compat

# Clean up possible left-over output from previous (failed) tests
try rm("hello.job", recursive=true) end
try rm("hello2.job", recursive=true) end
try rm("hello3.job", recursive=true) end
try rm("hello4.job", recursive=true) end
try rm("Trash", recursive=true) end

hello = let
    msg = "Hello, World!"
    persist("hello", ProcessManager, nprocs=2) do
        sleep(1)
        println(msg)
        40 + nworkers()
    end
end
@test status(hello) == job_running
@unix_only @test ismatch(r"sh hello", jobinfo(hello))
wait(hello)
@test status(hello) == job_done
@test getstdout(hello) == "Hello, World!\n"
@test getstderr(hello) == ""
result = fetch(hello)
@test result == 42

hello1 = readmgr("hello")
@test status(hello1) == job_done
cleanup(hello1)

hello = let
    msg = "Hello, World!"
    @persist "hello2" ProcessManager begin
        sleep(1)
        println(msg)
        42
    end
end
@test status(hello) == job_running
cancel(hello)
wait(hello)
@test status(hello) in (job_done, job_failed)
cleanup(hello)



# TODO: Test MPI



# Test PBSManager only if PBS is available
have_pbs = false
try
    run(pipeline(`qstat`, stdout=DevNull, stderr=DevNull))
    have_pbs = true
end

if have_pbs
    hello = persist("hello3", PBSManager, nprocs=2) do
        sleep(1)
        println("Hello, World!")
        "Hello, World!"
    end
    @test status(hello) in [job_queued, job_running]
    @unix_only @test ismatch(r"hello", jobinfo(hello))
    wait(hello)
    @test status(hello) == job_done
    @test fetch(hello) == "Hello, World!"
    @test getstdout(hello) == "Hello, World!\n"
    @test getstderr(hello) == ""
    cleanup(hello)
end



# Test SlurmManager only if Slurm is available
have_slurm = false
try
    run(pipeline(`sinfo`, stdout=DevNull, stderr=DevNull))
    have_slurm = true
end

if have_slurm
    hello = persist("hello4", SlurmManager, nprocs=2) do
        sleep(1)
        println("Hello, World!")
        "Hello, World!"
    end
    @test status(hello) in [job_queued, job_running]
    @unix_only @test ismatch(r"hello", jobinfo(hello))
    wait(hello)
    @test status(hello) == job_done
    @test fetch(hello) == "Hello, World!"
    @test getstdout(hello) == "Hello, World!\n"
    @test getstderr(hello) == ""
    cleanup(hello)
end



# Ensure all output directories have been removed

# TODO: Use glob instead of shell
@unix_only @test readstring(`sh -c 'echo hello*'`) == "hello*\n"
