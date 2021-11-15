#time "on"
#r "nuget: Akka.Remote, 1.4.25"
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp, 1.4.25"
#r "nuget: Akka.Serialization.Hyperion, 1.4.25"
#r "nuget: Akka.TestKit, 1.4.25"

open System
open System.Security.Cryptography
open Akka.Actor
open Akka.FSharp
open System.Threading

let PeersN = int(fsi.CommandLineArgs.[1])
let ReqN = int(fsi.CommandLineArgs.[2])

let dummy = bigint(Int64.MinValue)
let mutable stop = false

type BossActorMessages =
    | BossActorInit of nPeers : int * noOfRequests : int
    | UpdateHops of hops : int
    | PrintingHops

type MessagingPeer = 
    | PeerCreation of idBits : int * n : bigint * isFirst : bool
    | SuccessorFind of peerStart : bigint * id : bigint * queryType : int * fingerIndex : int * hops : int
    | SuccessorFound of bigint
    | FingerSuccessorUpdation of successor : bigint * idx : int
    | SendMsgPredecessor
    | PredecessorMsgReceive of bigint
    | Alert of bigint
    | Join of bigint
    | Maintain
    | StartMaintenance
    | FoundKey of hops : int
    | ReqCount of int
    | FingersToBeFixed

let system = System.create "system" <| Configuration.load ()
let akkaRouting = "akka://system/user/"


module numberGenerator = 
    let getDecimalValues (arr: byte[]) : bigint =
        let mutable multiplicator = 1I
        let mutable twoN = 2I
        let mutable result = 0I
        for i in arr.Length-1 .. -1 .. 0 do
            let mutable bytN = int(arr.[i])
            for _ in 1..8 do
                let bit = bigint(bytN &&& 1)
                result <- result + (bit * multiplicator)
                multiplicator <- multiplicator * twoN
                bytN <- bytN >>> 1
        result

    let nRandomForId (num : int) : bigint[] =
        Array.init<bigint> num (fun _ -> 
            let num: byte[] = Array.zeroCreate 20
            Random().NextBytes(num)
            let hashed = SHA1Managed.Create().ComputeHash(num)
            getDecimalValues(hashed)
        )

    let thePowerOfTwo (n : int) : bigint =
        bigint(Math.Pow(2.0, n |> float))

module mainFunction = 
    let BossActorRef = system.ActorSelection(akkaRouting+"BossActor")
    let bossActor (mailbox: Actor<_>) = 
        let mutable hops = 0
        let mutable peersNo = 0
        let mutable received = 0
        let mutable requestsNo = 0
        let rec loop() = actor{
            let! message = mailbox.Receive()
            match message with
            
            | BossActorInit(nPeers, nRequests) ->
                peersNo <- nPeers
                requestsNo <- nRequests

            | UpdateHops(totalHops)->
                hops <- hops + totalHops
                received <- received + 1
                if received = peersNo then
                    BossActorRef <! PrintingHops
            
            | PrintingHops ->
                let result = double(hops) / double(peersNo * requestsNo)
                printfn "The average hop count is %f" result
                stop <- true

            return! loop()
        }
        loop()
    
    let chordProtocol (mailbox: Actor<_>) = 
        let mutable parent = bigint(-1)
        let mutable child = bigint(-1)
        let mutable next = 0
        let mutable fingersTable : bigint[] = Array.empty
        let mutable n = bigint(0)
        let mutable m = bigint(0)
        let mutable idLen = 0
        let mutable hopTotal = 0
        let mutable requestsTotal = 0

        let cancelToken = new CancellationTokenSource()
        let rec loop() = actor{
            let! message = mailbox.Receive()
            let sender = mailbox.Sender()
            match message with

            | PeerCreation (idBits, N, isFirst) ->
                n <- N
                m <- numberGenerator.thePowerOfTwo idBits
                idLen <- idBits
                fingersTable <- [|for _ in 1 .. idLen -> dummy|]
                if isFirst then
                    parent <- N; child <- N
                else
                    child <- dummy; parent <- dummy


            | SuccessorFind(peerStart, id, queryNo, fingerIndex, hops) ->
                let foundIt = ((n < child) && ((id > n) && (id <= child)) || ((id > n) || (id <= child)))
                if foundIt then
                    let originPeerRef = system.ActorSelection(akkaRouting + string(peerStart))
                    if queryNo = 1 then originPeerRef <! SuccessorFound(child)
                    elif queryNo = 2 then originPeerRef <! FingerSuccessorUpdation(child, fingerIndex)
                    elif queryNo = 3 then originPeerRef <! FoundKey(hops)
                else
                    let ans = 
                        fingersTable |> Seq.tryFindBack(fun (s: bigint) -> 
                            (s <> dummy && ((n >= id && (s > n || ((s < n) && (s < id)))) || ((s > n) && (s < id)))))

                    let nextChild = (if ans.IsNone then n else ans.Value)
                    let succPeer = system.ActorSelection(akkaRouting + string(nextChild))
                    succPeer <! SuccessorFind(peerStart, id, queryNo, fingerIndex, hops + 1)

            | SuccessorFound(successor) ->
                child <- successor

            | SendMsgPredecessor ->
                if parent <> dummy then
                    sender.Tell(PredecessorMsgReceive(parent))

            | PredecessorMsgReceive(predecessor) ->
                let changeInSuccessor = ((n>= child && ((predecessor > n) || (predecessor < child))) || ((predecessor > n) && (predecessor < child)))
                if changeInSuccessor then
                    child <- predecessor
                let succPeer = system.ActorSelection(akkaRouting + string(child))
                succPeer <! Alert(n)

            | Alert(peer) ->
                let changeInPredecessor = ((parent = n) || (parent = dummy) || (parent >= n && ((peer > parent) || (peer < n))) || ((peer > parent) && (peer < n)))
                if changeInPredecessor then parent <- peer

            | Join(chordPeer) ->
                let chordPeer = system.ActorSelection(akkaRouting + string(chordPeer))
                chordPeer <! SuccessorFind(n, n, 1, 0, 0)
                
            | Maintain ->
                if child <> dummy then
                    let succ = system.ActorSelection(akkaRouting + string(child))
                    succ <! SendMsgPredecessor   

            | StartMaintenance ->
                let stab = async{
                    while true do
                        let peerActor = system.ActorSelection(akkaRouting + string(n))
                        peerActor <! Maintain
                        peerActor <! FingersToBeFixed
                        do! Async.Sleep 1000
                }
                Async.Start(stab, cancelToken.Token)

            | ReqCount(n) ->
                requestsTotal <- n

            | FingersToBeFixed ->
                if child <> dummy then
                    next <- next + 1
                    if next > idLen then
                        next <- 1

                    let mutable v = numberGenerator.thePowerOfTwo (next - 1)
                    v <- (n + v) % m

                    let path = akkaRouting + string(n)
                    let currPeer = system.ActorSelection(path)
                    currPeer <! SuccessorFind(n, v, 2, next, 0)

            | FingerSuccessorUpdation(succ, fingerIndex) ->
                fingersTable.[fingerIndex - 1] <- succ

            | FoundKey(hops) ->
                hopTotal <- hopTotal + hops
                requestsTotal <- requestsTotal - 1
                if requestsTotal = 0 then
                    BossActorRef <! UpdateHops(hopTotal)

            return! loop()
        }
        loop()


let BossActorRef =  spawn system "BossActor" <| mainFunction.bossActor 
BossActorRef <! BossActorInit(PeersN, ReqN)
let mutable peers = []
let cancel = new CancellationTokenSource()
let fingerCancel = new CancellationTokenSource()
let mutable firstChordPeer = dummy

module actionsSet1 = 
    let maintain = async{
        while true do
            for peer in peers do
                let peerActor = system.ActorSelection(akkaRouting + peer)
                peerActor <! Maintain
            do! Async.Sleep 100
    }

    let updateFinger = async{
        while true do
            for peer in peers do
                let peerActorRef = system.ActorSelection(akkaRouting + peer)
                peerActorRef <! FingersToBeFixed
            do! Async.Sleep 100
    }

module actionSet2 = 
    let funcSync = 
        Async.Start(actionsSet1.maintain, cancel.Token)
        Async.Start(actionsSet1.updateFinger, fingerCancel.Token)

    let keyFinding (peerIdentifier, key)  = 
        let peerRef = system.ActorSelection(akkaRouting + string(peerIdentifier))
        peerRef <! SuccessorFind(peerIdentifier, key, (Math.Pow(2.0, 1.0)|>int) + 1, 0, 0)


module finalFunc =
    let mutable x = 1
    let nextPeer = numberGenerator.nRandomForId PeersN
    
    let peerActor = spawn system (string(nextPeer.[0])) <| mainFunction.chordProtocol
    peers <- peers @ [string(nextPeer.[0])]
    firstChordPeer <- nextPeer.[0]
    peerActor <! PeerCreation((Math.Pow(2.0, 4.0)|>int) * 10, nextPeer.[0], true)
    peerActor <! ReqCount(ReqN) 
    
    for i in 1..nextPeer.Length - 1 do
        let peerActor = spawn system (string(nextPeer.[i])) <| mainFunction.chordProtocol
        peers <- peers @ [string(nextPeer.[i])]
        peerActor <! PeerCreation((Math.Pow(2.0, 4.0)|>int) * 10, nextPeer.[i], false)
        peerActor <! Join(firstChordPeer)
        peerActor <! ReqCount(ReqN)
    
    let requests = numberGenerator.nRandomForId ReqN
    
    for i in 0..nextPeer.Length - 1 do
        let peerRef = system.ActorSelection(akkaRouting + string(nextPeer.[i]))
        for j in 0..requests.Length - 1 do
            peerRef <! SuccessorFind(nextPeer.[i], requests.[j], (Math.Pow(2.0, 1.0)|>int) + 1, 0, 0)   
    
    while not stop do
        x <- 1