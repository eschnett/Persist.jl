using Persist
using Base.Test

# Clean up possible left-over output from previous (failed) tests
try rm("hello.job", recursive=true) end

hello = persist("hello", ProcessManager, 1) do
    sleep(1)
    println("Hello, World!")
end
@test status(hello) == :running
@unix_only @test ismatch(r"sh hello", jobinfo(hello))
waitjob(hello)
@test status(hello) == :done
@test getstdout(hello) == "Hello, World!\n"
@test getstderr(hello) == ""

hello1 = readmgr("hello")
@test status(hello1) == :done
cleanup(hello1)

const msg = "Hello, World!"
hello = @persist "hello" ProcessManager 1 begin
    sleep(1)
    println(msg)
end
@test status(hello) == :running
cancel(hello)
waitjob(hello)
@test status(hello) == :done
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
    hello = persist("hello", SlurmManager, 1) do
        sleep(1)
        println("Hello, World!")
    end
    @test status(hello) in [:queued, :running]
    @unix_only @test ismatch(r"hello", jobinfo(hello))
    waitjob(hello)
    @test status(hello) == :done
    @test getstdout(hello) == "Hello, World!\n"
    @test getstderr(hello) == ""
    cleanup(hello)
end
