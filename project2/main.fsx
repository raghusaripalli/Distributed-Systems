//import all the libraries
#time "on"
#r "nuget: Akka.Remote, 1.4.25"
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp, 1.4.25"
#r "nuget: Akka.Serialization.Hyperion, 1.4.25"
#r "nuget: Akka.TestKit, 1.4.25"
open System.Security.Cryptography
open System.Diagnostics
open System
open System.Collections.Generic
open System.Linq
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit
open Akka.Routing

type Gossip =
    |NeighborsSet of IActorRef[] 
    |StartGossip of String
    |GossipMsgConverge of String
    |SetValues of int * IActorRef[] * int
    |PushSumAlgo of float * float
    |StartPushSum of int
    |PushSumMsgConverge of float * float
    |PrintingN of int

let arg1 = fsi.CommandLineArgs.[1] // Reads the number N from input as a string.
let n = arg1 |> int //NumberOfNodes --> Number of actors involved
let topology = fsi.CommandLineArgs.[2] // FullNetwork or 3D grid or Line or Imperfect 3D grid
let algorithm = fsi.CommandLineArgs.[3] //Gossip or Push-sum
let system = ActorSystem.Create("Gossip")

let r  = System.Random()
let timer = Stopwatch()
let mutable conTime=0

let Listener (mailbox:Actor<_>) =
    let mutable msgCount = 0
    let mutable nodeCount = 0
    let mutable startTime = 0
    let mutable totalNodes =0
    let mutable allNodes:IActorRef[] = [||]

    let rec loop() = actor {
            let! message = mailbox.Receive()
            match message with 

            |GossipMsgConverge message ->
                let endTime = System.DateTime.Now.TimeOfDay.Milliseconds
                msgCount <- msgCount + 1

                if msgCount = totalNodes then
                    let rTime = timer.ElapsedMilliseconds
                    printfn "Time for Convergence from timer: %A ms" rTime
                    printfn "Time for Convergence from System Time: %A ms" (endTime-startTime)
                    conTime <- endTime - startTime
                    Environment.Exit 0

                else
                    let newStart= r.Next(0,allNodes.Length)
                    allNodes.[newStart] <! StartGossip("Hello")

            |PushSumMsgConverge (s, w) ->
                let endTime = System.DateTime.Now.TimeOfDay.Milliseconds
                nodeCount <- nodeCount + 1

                if nodeCount = totalNodes then
                    let rTime = timer.ElapsedMilliseconds
                    printfn "Time for Convergence from timer: %A ms" rTime
                    printfn "Time for Convergence from System Time: %A ms" (endTime-startTime)
                    conTime <-endTime-startTime
                    Environment.Exit 0

                else
                    let newStart=r.Next(0,allNodes.Length)
                    allNodes.[newStart] <! PushSumAlgo(s,w)
            
            |SetValues (strtTime,nodesRef,totNds) ->
                startTime <-strtTime
                allNodes <- nodesRef
                totalNodes <-totNds    
            | _->()

            return! loop()
        }
    loop()

    

let AlgoRun listener nodeNum (mailbox:Actor<_>)  =
    let mutable msgCount = 0 
    let mutable nbs:IActorRef[]=[||]

    let mutable sum1= nodeNum |> float
    let mutable weight = 1.0
    let mutable termRound = 1.0
    let mutable flag = 0
    let mutable counter = 1

    let mutable r1 = 0.0
    let mutable r2 = 0.0
    let mutable r3 = 0.0
    let mutable r4 = 0.0
    let mutable convergenceflag = 0
    let ratiolimit = 10.0**(-10.0)
    
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |NeighborsSet nArray->
                nbs<-nArray

        |PrintingN num->
                printfn "%A" nbs

        |StartGossip msg ->
                msgCount<- msgCount+1
                if(msgCount = 10) then
                      listener <! GossipMsgConverge(msg)
                else
                      let index= r.Next(0,nbs.Length)
                      nbs.[index] <! StartGossip(msg)

        |StartPushSum ind->
            let index = r.Next(0,nbs.Length)
            let start = index |> float
            nbs.[index] <! PushSumAlgo(start,1.0)

        |PushSumAlgo (s,w)->
            if(convergenceflag = 1) then
                let index = r.Next(0,nbs.Length)
                nbs.[index] <! PushSumAlgo(s,w)
            
            if(flag = 0) then
                if(counter = 1) then
                    r1<-sum1/weight
                else if(counter = 2) then
                    r2<-sum1/weight
                else if(counter = 3) then
                    r3<-sum1/weight
                    flag<-1
                counter<-counter+1

            sum1<-sum1+s
            sum1<-sum1/2.0
            weight<-weight+w
            weight<-weight/2.0
            
            r4<-sum1/weight
           
            if(flag=0) then
                let index= r.Next(0,nbs.Length)
                nbs.[index] <! PushSumAlgo(sum1,weight)                

            
            if(abs(r1-r4)<=ratiolimit && convergenceflag=0) then 
                      convergenceflag<-1
                      listener <! PushSumMsgConverge(sum1,weight)

            else
                      r1<-r2
                      r2<-r3
                      r3<-r4
                      let index= r.Next(0,nbs.Length)
                      nbs.[index] <! PushSumAlgo(sum1,weight)
        
        | _-> ()
        return! loop()
    }
    loop()

module Algorithm =               
    let callTheAlgo algorithm n nodeArray =
        (nodeArray : _ array)|>ignore
        if algorithm ="gossip" then 
            let starter= r.Next(0,n-1)
            nodeArray.[starter]<!StartGossip("Hello")
        elif algorithm ="push-sum" then
            let starter= r.Next(0,n-1)
            nodeArray.[starter]<!StartPushSum(starter)
        else
            printfn"Please enter a valid argument!"

            

module Topology =
    let listener= 
        Listener
            |> spawn system "listener"

    let mutable number = 0
    let rCount=int(ceil ((float n)** 0.33))
    let numNMod=rCount*rCount
    let numNM=int(numNMod)
    if(topology = "line" || topology = "full") then
        number <- n
    else
        number <- (numNM*rCount)

    let nodeArray = Array.zeroCreate(number)
    let mutable neighboursArray:IActorRef[]=Array.empty
    if(number = n) then
        for i in [0..n-1] do
                nodeArray.[i]<- AlgoRun listener (i+1)
                    |> spawn system ("Node"+string(i))
    else
        for i in [0..int(numNM*rCount)-1] do
                    nodeArray.[i]<- AlgoRun listener (i+1)
                                        |> spawn system ("Node"+string(i))

    let algoForThisTopo algorithm numN nodeArray =
        timer.Start()      
        listener<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodeArray,n)
        Algorithm.callTheAlgo algorithm numN nodeArray

    let fullTopo numN algorithm =
        for i in [0..numN-1] do
            if i=0 then
                neighboursArray<-nodeArray.[1..numN-1]
                nodeArray.[i]<!NeighborsSet(neighboursArray)

            elif i=(numN-1) then 
                neighboursArray<-nodeArray.[0..(numN-2)]
                nodeArray.[i]<!NeighborsSet(neighboursArray)
            else
                neighboursArray<-Array.append nodeArray.[0..i-1] nodeArray.[i+1..numN-1]
                nodeArray.[i]<!NeighborsSet(neighboursArray)
       
        algoForThisTopo algorithm numN nodeArray

    let lineTopo numN algo =
        for i in [0..numN-1] do
            if i=0 then
                neighboursArray<-nodeArray.[1..1]    
                nodeArray.[i]<!NeighborsSet(neighboursArray)
            elif i=(numN-1) then 
                neighboursArray<-nodeArray.[(numN-2)..(numN-2)]                   
                nodeArray.[i]<!NeighborsSet(neighboursArray)
            else
                neighboursArray<-Array.append nodeArray.[i-1..i-1] nodeArray.[i+1..i+1]            
                nodeArray.[i]<!NeighborsSet(neighboursArray)
        algoForThisTopo algorithm numN nodeArray

    let build3DTopo i step layer cycle = 
        // printfn "%d %d" layer (step+i) 
        let mutable nbArray:IActorRef[]=Array.empty
        if i=0 then
            nbArray<-Array.append nodeArray.[step+i+1..step+i+1] nodeArray.[step+i+rCount..step+i+rCount]

        elif i=rCount-1 then 
            nbArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+rCount..step+i+rCount]
        
        elif i=numNM-rCount then 
            nbArray<-Array.append nodeArray.[step+i+1..step+i+1] nodeArray.[step+i-rCount..step+i-rCount]

        elif i=numNM-1 then 
            nbArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i-rCount..step+i-rCount]

        elif i<rCount-1 then 
            nbArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+1..step+i+1] 
            nbArray<-Array.append nbArray nodeArray.[step+i+rCount..step+i+rCount] 

        elif i>numNM-rCount && i<numNM-1 then 
            nbArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+1..step+i+1] 
            nbArray<-Array.append nbArray nodeArray.[step+i-rCount..step+i-rCount] 

        elif i%rCount = 0 then 
            nbArray<-Array.append nodeArray.[step+i+1..step+i+1] nodeArray.[step+i-rCount..step+i-rCount] 
            nbArray<-Array.append nbArray nodeArray.[step+i+rCount..step+i+rCount] 

        elif (i+1)%rCount = 0 then 
            nbArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i-rCount..step+i-rCount] 
            nbArray<-Array.append nbArray nodeArray.[step+i+rCount..step+i+rCount] 
       
        else
            nbArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+1..step+i+1] 
            nbArray<-Array.append nbArray nodeArray.[step+i-rCount..step+i-rCount] 
            nbArray<-Array.append nbArray nodeArray.[step+i+rCount..step+i+rCount] 

        if layer =0 then
            nbArray <- Array.append nbArray nodeArray.[i+numNM..i+numNM]
        elif layer=cycle-1 then
            nbArray <- Array.append nbArray nodeArray.[step+i-numNM..step+i-numNM]
        else
            nbArray <- Array.append nbArray nodeArray.[step+i-numNM..step+i-numNM]
            nbArray <- Array.append nbArray nodeArray.[step+i+numNM..step+i+numNM]
        nodeArray.[step+i]<!NeighborsSet(nbArray)

    let buildImp3D i step layer cycle = 
        let mutable nbArray:IActorRef[]=Array.empty
        if i=0 then
            nbArray<-Array.append nodeArray.[step+i+1..step+i+1] nodeArray.[step+i+rCount..step+i+rCount]

        elif i=rCount-1 then 
            nbArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+rCount..step+i+rCount]

        elif i=numNM-rCount then 
            nbArray<-Array.append nodeArray.[step+i+1..step+i+1] nodeArray.[step+i-rCount..step+i-rCount]

        elif i=numNM-1 then 
            nbArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i-rCount..step+i-rCount]

        elif i<rCount-1 then 
            nbArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+1..step+i+1] 
            nbArray<-Array.append nbArray nodeArray.[step+i+rCount..step+i+rCount]                  

        elif i>numNM-rCount && i<numNM-1 then 
            nbArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+1..step+i+1] 
            nbArray<-Array.append nbArray nodeArray.[step+i-rCount..step+i-rCount] 

        elif i%rCount = 0 then 
            nbArray<-Array.append nodeArray.[step+i+1..step+i+1] nodeArray.[step+i-rCount..step+i-rCount] 
            nbArray<-Array.append nbArray nodeArray.[step+i+rCount..step+i+rCount] 

        elif (i+1)%rCount = 0 then 
            nbArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i-rCount..step+i-rCount] 
            nbArray<-Array.append nbArray nodeArray.[step+i+rCount..step+i+rCount] 
       
        else
            nbArray<-Array.append nodeArray.[step+i-1..step+i-1] nodeArray.[step+i+1..step+i+1] 
            nbArray<-Array.append nbArray nodeArray.[step+i-rCount..step+i-rCount] 
            nbArray<-Array.append nbArray nodeArray.[step+i+rCount..step+i+rCount] 

        if layer =0 then
            nbArray <- Array.append nbArray nodeArray.[i+numNM..i+numNM]
            nbArray <- Array.append nbArray nodeArray.[i+numNM+1..i+numNM+1]
        elif layer=cycle-1 then
            nbArray <- Array.append nbArray nodeArray.[step+i-numNM..step+i-numNM]
            nbArray <- Array.append nbArray nodeArray.[step+i-numNM-1..step+i-numNM-1]
        else
            nbArray <- Array.append nbArray nodeArray.[step+i-numNM..step+i-numNM]
            nbArray <- Array.append nbArray nodeArray.[step+i+numNM..step+i+numNM]
            nbArray <- Array.append nbArray nodeArray.[step+i+numNM-1..step+i+numNM-1]
        nodeArray.[step+i]<!NeighborsSet(nbArray)

    let build3DNetwork numN topology algorithm = 
        let mutable cycle = ((((numN |> float) ** 0.33) |> ceil) ** 2.0) |> int
        let mutable step = 0

        // printfn "%A" nodeArray
        printfn "%d %d %d" cycle rCount step
        if(topology = "3D") then
            for layer in [1..rCount] do
                for i in [0..numNM-1] do
                    build3DTopo i step layer cycle 
                step <- step+cycle
        else
            for layer in [1..rCount] do
                for i in [0..numNM-1] do
                    buildImp3D  i step layer cycle 
                step <- step+cycle         
            

    let topoFor3D numN topology algorithm =
        build3DNetwork numN topology algorithm
        algoForThisTopo algorithm numN nodeArray 
                               
printfn "%s %s" topology algorithm
if topology ="full" then Topology.fullTopo n algorithm
elif topology ="line" then Topology.lineTopo n algorithm
elif topology ="3D" || topology = "imp3D" then Topology.topoFor3D n topology algorithm
else printfn "Please enter a valid argument!"
System.Console.ReadLine() |> ignore