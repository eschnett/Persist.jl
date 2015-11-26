using Persist
using Base.Test

# Clean up possible left-over output from previous (failed) tests
try rm("hello.job", recursive=true) end
try rm("hello2.job", recursive=true) end
try rm("hello3.job", recursive=true) end
try rm("Trash", recursive=true) end

hello = persist("hello", ProcessManager) do
    sleep(1)
    println("Hello, World!")
    42
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

const msg = "Hello, World!"
const res = result
hello = @persist "hello2" ProcessManager begin
    sleep(1)
    println(msg)
    res
end
@test status(hello) == job_running
cancel(hello)
wait(hello)
@test status(hello) == job_done
@test fetch(hello) == result
cleanup(hello)
# TODO: Use glob instead of shell
@unix_only @test readall(`sh -c 'echo hello*'`) == "hello*\n"

# Test SlurmManager only if Slurm is installed
have_slurm = false
try
    run(pipeline(`sinfo`, stdout=DevNull, stderr=DevNull))
    have_slurm = true
end

if have_slurm
    hello = persist("hello3", SlurmManager, nprocs=2) do
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
