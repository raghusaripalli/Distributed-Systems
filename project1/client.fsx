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

// Configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            log-config-on-start : on
            stdout-loglevel : DEBUG
            loglevel : ERROR

            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                debug : {
                    receive : on
                    autoreceive : on
                    lifecycle : on
                    event-stream : on
                    unhandled : on
                }
            }
            remote {
                helios.tcp {
                    port = 8778
                    hostname = localhost
                }
            }
        }")
let proc = Process.GetCurrentProcess()
let cpuTimeStamp = proc.TotalProcessorTime
let timer = Stopwatch()
let mutable actN = ""
let mutable workers = Environment.ProcessorCount |> int
let mutable count = 0
let mutable totalCoins = 0L

let system = ActorSystem.Create("RemoteMiner", configuration)
let serverAddress = fsi.CommandLineArgs.[1]
let clientAddress = "localhost:8778"
let pref = system.ActorSelection(sprintf "akka.tcp://Miner@%s:8777/user/PrinterActor" serverAddress)

let rec appendAndIterate(bs: string, l: int, start: int, stop: int, N: int) =
        if l > 0 then 
            for c in start .. stop do
                let lessL = l - 1
                let ebs = bs + string (char c)
                appendAndIterate (ebs, lessL, start, stop, N)
                if l = 1 then
                    let s = System.Text.Encoding.ASCII.GetBytes(ebs) |> (new SHA256Managed()).ComputeHash |> Array.map (fun (x : byte) -> System.String.Format("{0:x2}", x)) |> String.concat String.Empty
                    let firstN = s.[0 .. N-1]
                    if firstN.Equals(actN) then
                        let cpuTime = (proc.TotalProcessorTime-cpuTimeStamp).TotalMilliseconds |> float
                        let realTime = timer.ElapsedMilliseconds |> float
                        totalCoins <- totalCoins + 1L
                        pref <! sprintf "RemoteIp: %s, Coin %d:-\n %s %s\nCPU Time: %fms\nReal Time: %fms\nRatio: %f" clientAddress totalCoins ebs s cpuTime realTime (cpuTime/realTime)

let RemoteChild(mailbox:Actor<_>) = 
    let rec loop()=actor {
        let! msg = mailbox.Receive()
        match msg with
        | WorkerMsg(bs, l, start, stop, n) -> appendAndIterate(bs, l, start, stop, n)
                                              if count = workers then
                                                count <- 0
                                                pref <! sprintf "Done %s %d" clientAddress workers
                                              else
                                                count <- count + 1
    }
    loop()

let RemoteBossActor = 
    spawn system "RemoteBossActor"
    <| fun mailbox ->
        let rec loop() =
            actor {
                let! (message:obj) = mailbox.Receive()
                let (bs, count, start, stop, i, j, n) : Tuple<string,int,int,int,int, int, int> = downcast message
                actN <- String.replicate n "0"
                let workersList=[for a in 1 .. workers do yield(spawn system ("Remote_Actor-"+(string a)) RemoteChild)]
                for k in i .. j-1 do
                    let ebs = bs + string (char k)
                    workersList.Item(k-i|>int) <! WorkerMsg (ebs, count, start, stop, n)
                
                return! loop()
            }
        printfn "Remote Boss Started \n" 
        loop()

timer.Start()
System.Console.ReadLine() |> ignore