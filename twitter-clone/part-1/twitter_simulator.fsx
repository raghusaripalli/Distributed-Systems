#time "on"
#r "nuget: Akka.Remote, 1.4.25"
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp, 1.4.25"
#r "nuget: Akka.Serialization.Hyperion, 1.4.25"
#r "nuget: Akka.TestKit, 1.4.25"

open Akka.Actor
open Akka.Configuration
open Akka.FSharp

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                
            }
            remote {
                helios.tcp {
                    port = 8778
                    hostname = localhost
                }
            }
        }")


let rand = System.Random()
let ipAddress = fsi.CommandLineArgs.[1] |> string
let port = fsi.CommandLineArgs.[2] |>string
let simulatorID = "NarutoUzumakiTheSimulator"
let noUsers = fsi.CommandLineArgs.[3] |> int
let address = "akka.tcp://TwitterEngine@" + ipAddress + ":" + port + "/user/RequestHandler"
let system = ActorSystem.Create("TwitterClient", configuration)
let server = system.ActorSelection(address)// let N = 100
let mutable isActive = Array.create noUsers false
let mutable workersForClient = []
let hashtags = [|"#TheTwitterProject";"#COP5615isgreat";"#IDontLikeHashTags";"#ActorModelWorksWellForThis";"#OkayByeNow";"#DOSP";"#Project";"#TwitterClone";"#Fall2021"; "#HashTagCultureIsNoMore";|]

module serverFunctions = 
    let StartSubscriptions order (workerList: list<IActorRef>) =
        for i in 0 .. order - 1 do
            server <! "Subscribe|"+workerList.[order - 1].Path.Name+"|"+workerList.[i].Path.Name

    let sendTheTweet (myref: IActorRef) rank (workersList: list<IActorRef>) login = 
        let mutable isRetweet = false
        let mutable login_no = login
        let temp = rand.Next(0,100)
        if temp % 50 = 25 then
            if login then
                login_no <- false
                server <! "Logout|"+myref.Path.Name+"|"
            else
                login_no <- true
                server <! "Login|"+myref.Path.Name+"|"
        elif temp = 31 then
            server <! "GetTweets|"+myref.Path.Name+"|"
        elif temp = 32 then
            server <! "GetMentions|"+myref.Path.Name+"|"
        elif temp = 33 then
            server <! "GetHashTags|"+myref.Path.Name+"|"+hashtags.[rand.Next(0,10)]+"|"
        else
            if temp%10 =0 then
                isRetweet <- true
            if isRetweet then
                let tweetmsg = "ReTweet|"+myref.Path.Name+"|"
                server <! tweetmsg
            else
                let tweetmsg = "Tweet|"+myref.Path.Name+"|" + "Here we are tweeting our way to glory! @"+workersList.[rand.Next(0,noUsers)].Path.Name+" "+ hashtags.[rand.Next(0,10)]    
                server <! tweetmsg
        let tspan = rank|>int
        system.Scheduler.ScheduleTellOnce(tspan ,myref,"SubscribeForTweets",myref)
        login_no
    
    let Printer (mailbox:Actor<_>) = 
        let rec loop() = actor{
            let! msgs = mailbox.Receive()
            printfn "%s" msgs
            return! loop()
        }
        loop()
    let printRef = spawn system "Print" Printer


type SimulatorMsg =
    | StartSimulation
    | StartAcknowledgement 
    | SubsAcknowledgement

module serverWork = 

    let mutable simulation = null
    let mutable userLogin = true
    let mutable cid = 0
    let mutable order = 0

    let ClientActor (mailbox:Actor<_>)=
        let rec loop() = actor{
            let! msg = mailbox.Receive()
            let received = msg|>string
            let message = (received).Split '|'

            let clientOps =  
                    
                if message.[0].CompareTo("Ack") = 0 then
                    if message.[1].CompareTo("Register") = 0 then
                        simulation <! StartAcknowledgement 
                    elif message.[1].CompareTo("Subscribe") = 0 then
                        simulation <! SubsAcknowledgement

                elif message.[0].CompareTo("SubscribeForTweets") = 0 then
                    cid <- cid + 1
                    userLogin <- serverFunctions.sendTheTweet mailbox.Self order workersForClient userLogin
                
                elif message.[0].CompareTo("StartZipf") = 0 then 
                    order <- message.[1] |> int
                    server<!"Register|"+mailbox.Self.Path.Name
                    server<!"Login|"+mailbox.Self.Path.Name
                    simulation <- mailbox.Sender()

                elif message.[0].CompareTo("StartSubscriptions") = 0 then
                    serverFunctions.StartSubscriptions order workersForClient
                
                elif message.[0].CompareTo("GetResponse") = 0 then
                    printfn "%A" msg

                elif message.[0].CompareTo("QUIT") = 0 then
                    mailbox.Context.System.Terminate() |> ignore 
                    
            return! loop()
        }
        loop()
    workersForClient <- [for a in 1 .. noUsers do yield(spawn system (simulatorID + "_clientNo_" + (string a)) ClientActor)]

    let mutable startAcknowledgementCount = 0
    let mutable ackCountForTweets = 0
    let mutable ackCountForSubscribers = 0

    let SimulatorActor (mailbox:Actor<_>)=
        let rec loop() = actor{
            let! msg = mailbox.Receive()
            match msg with 
            | StartSimulation ->for i in 0 .. noUsers-1 do
                                workersForClient.[i] <! "StartZipf|"+(string (i+1))
            | StartAcknowledgement ->   startAcknowledgementCount <- startAcknowledgementCount + 1
                                        if startAcknowledgementCount = noUsers then
                                            printfn "Initialization Done"
                                        for i in 0 .. noUsers-1 do
                                            workersForClient.[i] <! "StartSubscriptions|"
            | SubsAcknowledgement ->    ackCountForSubscribers <- ackCountForSubscribers + 1
                                        if ackCountForSubscribers = noUsers then
                                            printfn "Subscribe activities Done"
                                            for i in 0 .. noUsers-1 do
                                                workersForClient.[i] <! "SubscribeForTweets|"
            
            return! loop();
        }
        loop()


let simulatorReference = spawn system "simulator" serverWork.SimulatorActor
simulatorReference <! StartSimulation
system.WhenTerminated.Wait()
