using Persist
using Base.Test

const isunix = @unix ? true : false

try rm("hello.job", recursive=true) end

hello = persist("hello", ProcessManager, 1) do
  sleep(1)
  println("Hello, World!")
end
@test status(hello) == :running
if isunix
  @test ismatch(r"sh hello", jobinfo(hello))
end
waitjob(hello)
@test status(hello) == :done
@test getstdout(hello) == "Hello, World!\n"
@test getstderr(hello) == ""

hello1 = readmgr("hello")
@test status(hello1) == :done
cleanup(hello1)

hello = persist("hello", ProcessManager, 1) do
  sleep(1)
  println("Hello, World!")
end
@test status(hello) == :running
cancel(hello)
waitjob(hello)
@test status(hello) == :done
cleanup(hello)
# TODO: Use glob instead of shell
if isunix
  @test readall(`sh -c 'echo hello*'`) == "hello*\n"
end
