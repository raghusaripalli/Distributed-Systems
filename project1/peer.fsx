#time "on"
#r "nuget: Akka.Remote, 1.4.25"
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp, 1.4.25"
#r "nuget: Akka.Serialization.Hyperion, 1.4.25"
open System.Security.Cryptography
open System
open Akka.Actor
open Akka.FSharp 
open Akka.Configuration
open System.Diagnostics

type ActorMsg =
    | WorkerMsg of string*int*int*int*int
    | DispatcherMsg of string*int
    | EndMsg
    | PrinterActor of string

// Configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            log-config-on-start : on
            stdout-loglevel : DEBUG
            loglevel : ERROR
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            }
            remote.helios.tcp {
                transport-protocol = tcp
                port = 8777
                hostname = 10.20.216.17
            }
        }")

let system = ActorSystem.Create("Miner", configuration)
let proc = Process.GetCurrentProcess()
let cpuTimeStamp = proc.TotalProcessorTime
let timer = Stopwatch()
let baseString = "rsaripalli;"
let n = fsi.CommandLineArgs.[1] |> int
printfn "%d" n
let actN = String.replicate n "0"
let mutable count = 0;
let mutable totalCoins = 0L;

let rec appendAndIterate(bs: string, l: int, start: int, stop: int, N: int) =
    if l > 0 then 
        for c in start .. stop do
            let lessL = l - 1
            let ebs = bs + string (char c)
            appendAndIterate (ebs, lessL, start, stop, N)
            if l = 1 then
                let s = System.Text.Encoding.ASCII.GetBytes(ebs) |> (new SHA256Managed()).ComputeHash |> Array.map (fun (x : byte) -> System.String.Format("{0:x2}", x)) |> String.concat String.Empty
                let firstN = s.[0 .. N-1]
                // match firstN with
                // | actN -> ebs 
                if firstN.Equals(actN) then
                    let cpuTime = (proc.TotalProcessorTime-cpuTimeStamp).TotalMilliseconds |> float
                    let realTime = timer.ElapsedMilliseconds |> float
                    totalCoins <- totalCoins + 1L
                    printfn "Coin %d:-\n %s %s\nCPU Time: %fms\nReal Time: %fms\nRatio: %f" totalCoins ebs s cpuTime realTime (cpuTime/realTime)


//worker actor
let generateString (mailbox:Actor<_>)=
    // let pref = system.ActorSelection("akka.tcp://Miner@10.20.195.23:8777/user/PrinterActor")
    let rec loop()=actor{
        let! msg = mailbox.Receive()
        match msg with 
        | WorkerMsg(bs,count, start, stop, N) -> 
                                                 appendAndIterate (bs, count, start, stop, N)
                                                 mailbox.Sender()<! EndMsg //send back the finish message to boss
        | _ -> printfn "Worker Received Wrong message"
    }
    loop()
let start = 45
let stop = 126
let step = 60* Environment.ProcessorCount |> int
let totalactors = stop - start
let length = 6
let mutable i = start
let boss (mailbox:Actor<_>) =
    let rec loop()=actor{
        let! msg = mailbox.Receive()
        match msg with 
        | DispatcherMsg(bs, N) ->
                                let workersList=[for a in 1 .. step do yield(spawn system ("Master_Actor-" + (string a)) generateString)]
                                for j in 0 .. (step-1) do
                                    let ebs = bs + string (char i)
                                    workersList.Item(j|>int) <! WorkerMsg(ebs, length, start, stop, N)
                                    i <- i + 1
        | EndMsg ->
                    count <- count + 1
                    if count = totalactors then //checking if all workers have already sent the end message
                        mailbox.Context.System.Terminate() |> ignore //terminating the actor system
                    else
                        DispatcherMsg(baseString, n) |> ignore
        | _ -> printfn "Dispatcher Received Wrong message"
        return! loop()
    }
    loop()

//creating boss actor
let bossRef = spawn system "boss" boss
bossRef <! DispatcherMsg(baseString, n)

let PrinterActor (mailbox:Actor<_>) = 
    let rec loop () = actor {
        let! (message:obj) = mailbox.Receive()
        if (message :? string) then
            if (string message).StartsWith("Done") then
                printfn "%s" (string message)
                bossRef <! EndMsg
            else
                printfn "%s" (string message)
        return! loop()
    }
    loop()
let printerRef = spawn system "PrinterActor" PrinterActor
timer.Start()
//waiting for boss actor to terminate the actor system
system.WhenTerminated.Wait()