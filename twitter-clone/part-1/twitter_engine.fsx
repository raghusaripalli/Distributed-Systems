#time "on"
#r "nuget: Akka.Remote, 1.4.25"
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp, 1.4.25"
#r "nuget: Akka.Serialization.Hyperion, 1.4.25"
#r "nuget: Akka.TestKit, 1.4.25"

open System
open System.Collections.Generic
open Akka.Actor
open Akka.Configuration
open Akka.FSharp


let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
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
                    port = 8777
                    hostname = localhost
                }
            }
        }")


let rand = System.Random()
let system = ActorSystem.Create("TwitterEngine", configuration)

let mutable noUser = Map.empty
let mutable no_active_users = Map.empty
let mutable tweets = Map.empty
let mutable tweet_owner = Map.empty
let mutable no_followers = Map.empty
let mutable tweets_by_subscribers = Map.empty
let mutable username_mentions = Map.empty
let mutable hashtags = Map.empty
let mutable mainIdentification = 1234
let ticksPerMicroSecond = (TimeSpan.TicksPerMillisecond |> float )/(1000.0)
let ticksPerMilliSecond = TimeSpan.TicksPerMillisecond |> float



module twitterUserFunctions = 
    module userIdStatus = 
        let UserisActive userID = 
            let result = no_active_users.TryFind userID
            result <> None

        let getUserID username =
            let found = noUser.TryFind username
            found
        
        let registerUser username = 
            noUser <- noUser.Add(username, mainIdentification|>string)
            let userID = mainIdentification |> string
            mainIdentification <- mainIdentification + rand.Next(1,32)
            userID

    module logInStatus = 
        let userLoggedIn username clientref= 
            let mutable userId = userIdStatus.getUserID username 
            let mutable UID = ""
            if userId = None then
                UID <- (userIdStatus.registerUser username) 
            else
                UID <- userId.Value
            let found = no_active_users.TryFind UID
            if found = None then
                no_active_users <- no_active_users.Add(UID,clientref)

        let userLoggedOut username = 
            let mutable userId = userIdStatus.getUserID username
            if userId <> None then
                no_active_users <- no_active_users.Remove(userId.Value)
    
    

module followerFunctions = 
    let followersAdd username follower request=
        let userId = twitterUserFunctions.userIdStatus.getUserID username
        if userId <> None then
            if (twitterUserFunctions.userIdStatus.UserisActive userId.Value) then
                let fid = twitterUserFunctions.userIdStatus.getUserID follower
                if fid<>None && userId <> None then
                    let fset = no_followers.TryFind fid.Value
                    if fset = None then
                        let followers = new List<string>()
                        followers.Add(userId.Value)
                        no_followers <- no_followers.Add(fid.Value, followers)
                    else 
                        fset.Value.Add(userId.Value)
                    request <! "Ack|Subscribe|"
            else
                request <! "Ack|NotLoggedIn|"

    let getFollowers userId = 
        let followers = no_followers.TryFind userId
        if followers <> None then
            followers.Value
        else
            let elist = new List<string>()
            elist


module tweetFunctions = 
    let tweetIt tweetID tweet = 
        tweets <- tweets.Add(tweetID,tweet)

    let tweetToUser userID tweet =
        let find = tweet_owner.TryFind userID
        if find = None then
            let list = new List<string>()
            list.Add(tweet)
            tweet_owner <- tweet_owner.Add(userID,list)
        else
            find.Value.Add(tweet)

    let mentions userId tweet user_mentioned =
        let tagId = twitterUserFunctions.userIdStatus.getUserID user_mentioned
        if tagId  <> None then
            let mentions_ = username_mentions.TryFind tagId .Value
            if mentions_ = None then
                let mutable mp = Map.empty
                let tlist = new List<string>()
                tlist.Add(tweet)
                mp <- mp.Add(userId ,tlist)
                username_mentions <- username_mentions.Add(tagId .Value,mp)
            else
                let noUser = mentions_.Value.TryFind userId 
                if noUser = None then
                    let tlist = new List<string>()
                    tlist.Add(tweet)
                    let mutable mp = mentions_.Value
                    mp <- mp.Add(userId ,tlist)
                    username_mentions <- username_mentions.Add(tagId .Value,mp)
                else
                    noUser.Value.Add(tweet)

    let sendTweetAcknowledgement userId tweet = 
        let status = no_active_users.TryFind userId
        if status <> None then
            status.Value <! "Tweet|Self|"+ tweet

    let getTweets userId =
        let tlist = tweet_owner.TryFind userId
        let response = new List<string>()
        if tlist <> None then
            tlist.Value
        else
            response

    let sendTweetsToActiveFollowers userId tweet =
        let followers = followerFunctions.getFollowers userId
        // printfn "no_followers list %A" flist
        for i in followers do
            let status = no_active_users.TryFind i
            if status <> None then
                // printfn "Sent to active client %A" i
                status.Value <! "Tweet|Subscribe|"+tweet

module subscriberFunctions = 
    let addSubscribersTweet tweet ownerId =
        let followers = followerFunctions.getFollowers ownerId
        for i in followers do
            let subscribers = tweets_by_subscribers.TryFind i        
            if subscribers = None then
                let stweets = new List<string>()
                stweets.Add(tweet)
                tweets_by_subscribers <- tweets_by_subscribers.Add(i,stweets)
            else
                subscribers.Value.Add(tweet)


module hashtagFunctions = 
    let HashTagging hashtag tweet =
        let find_hashtag = hashtags.TryFind hashtag
        if find_hashtag = None then
            let tweets = new List<string>()
            tweets.Add(tweet)
            hashtags <- hashtags.Add(hashtag, tweets)
        else
            find_hashtag.Value.Add(tweet)

    let getHashTags hashtag =
        let hashtags = hashtags.TryFind hashtag
        let result = new List<string>()
        
        if hashtags <> None then 
            // printfn "%i" hlist.Value.Count
            for i in hashtags.Value do
                // printfn "%s" i
                result.Add(i)
        result

    let concatList (list: List<string>) =
        let mutable response = ""
        let len = Math.Min(100,list.Count)
        for i in 0 .. len-1 do
            response <- response + list.[i]+"|"
        response

    let getMentions userId = 
        let tweets = username_mentions.TryFind userId
        let res = new List<string>()
        if tweets <> None then
            for i in tweets.Value do
                for j in i.Value do
                    res.Add(j)
        res


type RegisterMsg =
    | UserRegistration of string*IActorRef
    | Login of string*IActorRef
    | Logout of string*IActorRef

type TweetParserMsg = 
    | Parse of string*string*string*IActorRef

type FollowersMsg = 
    | Add of string*string*IActorRef
    | Update of string*string*IActorRef

type TweetHandlerMsg =
    | AddTweet of string*string*string*IActorRef
    | ReTweet of string*IActorRef

type GetHandlerMsg =
    | GetTweets of string*IActorRef
    | GetTags of string*IActorRef
    | GetHashTags of string * string * IActorRef

module handlers = 
    let handleRegistration (mailbox:Actor<_>)=
        let rec loop() = actor{
            let! msg = mailbox.Receive()
            // printfn "%A" msg
            try
                match msg with 
                | UserRegistration(username,userReference) ->   let UID = twitterUserFunctions.userIdStatus.getUserID username
                                                                let mutable cid = ""
                                                                if UID = None then
                                                                    cid <- twitterUserFunctions.userIdStatus.registerUser username
                                                                else
                                                                    cid <- UID.Value
                                                                userReference <! ("Ack|Register|"+(string cid))
                | Login(username,userReference) ->    twitterUserFunctions.logInStatus.userLoggedIn username userReference
                                                      userReference <! "Ack|Login|"
                | Logout(username,userReference) ->   twitterUserFunctions.logInStatus.userLoggedOut username
                                                      userReference <! "Ack|Logout|"
            finally
                let a = "Ignore"
                a |> ignore
            return! loop()
        }
        loop()

    let handleFollowers (mailbox:Actor<_>) = 
        
        let rec loop() = actor{
            let! msg = mailbox.Receive()
            try
                match msg with
                | Add(noUser,follower,request) ->     followerFunctions.followersAdd noUser follower request
                                                
                | Update (ownerid,tweet,request)->    subscriberFunctions.addSubscribersTweet tweet ownerid
            finally
                let a = "Ignore"
                a |> ignore                                     
            return! loop()
        }
        loop()

    let followerHAndlerActor = spawn system "Follower" handleFollowers
    
    module tweetHandler = 
        let parseTweets (mailbox:Actor<_>) = 
                let rec loop() = actor{
                    let! msg = mailbox.Receive()
                    try
                        match msg with 
                        | Parse(userId,tweetid,tweet,request) ->    let splits = (tweet).Split ' '
                                                                    for i in splits do
                                                                        if i.StartsWith "@" then
                                                                            let temp = i.Split '@'
                                                                            tweetFunctions.mentions userId tweet temp.[1]
                                                                        elif i.StartsWith "#" then
                                                                            hashtagFunctions.HashTagging i tweet
                                                                    request <! "Ack|Tweet|"
                    finally 
                        let a = "Ignore"
                        a |> ignore
                    return! loop()
                }
                loop()

        let tweetParsingActor = spawn system "parseTweets" parseTweets
        
        let mutable count = 0L
        let mutable timetaken = 0.0
        let mutable millisecs = 0
        let timer = System.Diagnostics.Stopwatch()
        let handleTweets (mailbox:Actor<_>) =
            let rec loop() = actor{
                let! msg = mailbox.Receive()

                try
                    match msg with
                    | AddTweet(username,request_id,tweet,request) ->    timer.Restart()
                                                                        let userId = twitterUserFunctions.userIdStatus.getUserID username
                                                                        count <- count + 1L
                                                                        if userId <> None then
                                                                            if (twitterUserFunctions.userIdStatus.UserisActive userId.Value) then
                                                                                let tweetid = count|>string
                                                                                tweetFunctions.tweetIt tweetid tweet
                                                                                tweetFunctions.tweetToUser userId.Value tweet
                                                                                tweetParsingActor <! Parse(userId.Value, tweetid, tweet, request)
                                                                                followerHAndlerActor <! Update(userId.Value, tweet, request)
                                                                                tweetFunctions.sendTweetAcknowledgement userId.Value tweet
                                                                                tweetFunctions.sendTweetsToActiveFollowers userId.Value tweet
                                                                            else
                                                                                request <! "Ack|NotLoggedIn|"
                                                                        timetaken <- timetaken + (timer.ElapsedTicks|>float)
                                                                        millisecs <- millisecs + (timer.ElapsedMilliseconds|>int)
                                                                        if count % 10000L = 0L then
                                                                            printfn "Mean time for computing Tweet after %i tweets took %A microseconds & total is %A milliseconds" count (timetaken/(ticksPerMicroSecond*(count|>float))) (timetaken/ticksPerMilliSecond) 
                                                                                
                    | ReTweet(username, request) -> let index = rand.Next(0,count|>int)|>string
                                                    let tweet = tweets.TryFind index
                                                    if tweet <> None then
                                                        mailbox.Self<! AddTweet(username,index,tweet.Value,request)
                                                    
                finally
                    let a = "Ignore "
                    a |> ignore
                return! loop()
            }
            loop()

    exception Error1 of string

let mainHandler(mailbox:Actor<_>)=
    let timer = System.Diagnostics.Stopwatch()
    let mutable gettweetstime = 0
    let mutable gettweetscnt =0
    let mutable gettagstime = 0
    let mutable gettagscnt =0
    let mutable gethashtagstime = 0
    let mutable gethashtagscnt =0
    let rec loop()=actor{
        let! msg = mailbox.Receive()
        try
            match msg with
            | GetTweets(username,request) ->    timer.Restart()
                                                gettweetscnt <- gettweetscnt + 1
                                                let userId = twitterUserFunctions.userIdStatus.getUserID username
                                                if userId <> None then
                                                    if twitterUserFunctions.userIdStatus.UserisActive userId.Value then
                                                        let resp = (tweetFunctions.getTweets userId.Value) 
                                                        request <! "GetResponse|GetTweets|"+(hashtagFunctions.concatList resp)+"\n"
                                                    else
                                                        request <! "Ack|NotLoggedIn|"
                                                gettweetstime <- gettweetstime + (timer.ElapsedTicks|>int)
                                                // printfn "%i gettweets count" gettweetscnt
                                                if gettweetscnt%100 = 0 then
                                                    printfn "Mean time for computing 'get tweets' took %A microseconds & after %i requests, total is %A milliseconds" ((gettweetstime|>float)/(ticksPerMicroSecond*(gettweetscnt|>float))) gettweetscnt ((gettweetstime|>float)/ticksPerMilliSecond)
            
            | GetTags(username,request)->   timer.Restart()
                                            gettagscnt <- gettagscnt + 1
                                            let userId = twitterUserFunctions.userIdStatus.getUserID username
                                            if userId <> None then
                                                if twitterUserFunctions.userIdStatus.UserisActive userId.Value then
                                                    let resp = (hashtagFunctions.getMentions userId.Value)
                                                    request <! "GetResponse|GetMentions|"+(hashtagFunctions.concatList resp)+"\n"
                                                else
                                                        request <! "Ack|NotLoggedIn|"
                                            gettagstime <- gettagstime + (timer.ElapsedTicks|>int)
                                            if gettagscnt%100 = 0 then
                                                    printfn "Mean time for computing 'get username_mentions' took %A microseconds & after %i requests, total is %A milliseconds" ((gettagstime|>float)/(ticksPerMicroSecond*(gettagscnt|>float))) gettagscnt ((gettagstime|>float)/ticksPerMilliSecond)
                
            | GetHashTags(username,hashtag,request)->   timer.Restart()
                                                        gethashtagscnt <- gethashtagscnt + 1
                                                        let userId = twitterUserFunctions.userIdStatus.getUserID username
                                                        if userId <> None then
                                                            if twitterUserFunctions.userIdStatus.UserisActive userId.Value then
                                                                let resp = (hashtagFunctions.getHashTags hashtag)
                                                                // printfn "%s" (concatList resp)
                                                                request <! "GetResponse|GetHashTags|"+(hashtagFunctions.concatList resp)+"\n"
                                                            else
                                                                    request <! "Ack|NotLoggedIn|"
                                                        gethashtagstime <- gethashtagstime + (timer.ElapsedTicks|>int)
                                                        if gethashtagscnt%100 = 0 then
                                                            printfn "Mean time for computing 'get hashtags' took %A microseconds & after %i requests, total is %A milliseconds" ((gethashtagstime|>float)/(ticksPerMicroSecond*(gethashtagscnt|>float))) gethashtagscnt ((gethashtagstime|>float)/ticksPerMilliSecond)
            
        with
            | :? System.InvalidOperationException as ex ->  let b = "ignore"
                                                            // printfn "exception"
                                                            b |>ignore


        return! loop()
    }
    loop()


let tweetHandlerActor = spawn system "tweetHandler" handlers.tweetHandler.handleTweets
let registrationHandlingActor = spawn system "registrationHandler" handlers.handleRegistration
let theMainHandlerActor = spawn system "mainHandler" mainHandler


module mainFunction = 
    let mainFunc = 
        spawn system "RequestHandler"
        <| fun mailbox ->
            let mutable request_id = 0
            let mutable ticks = 0L
            let timer = System.Diagnostics.Stopwatch()
            
            let rec loop() =
                actor {
                    let! msg = mailbox.Receive()
                    request_id <- request_id + 1
                    timer.Restart()
                    // printfn "%s" msg 
                    let operation = (msg|>string).Split '|'
                    if operation.[0].CompareTo("Register") = 0 then
                        registrationHandlingActor <! UserRegistration(operation.[1],mailbox.Sender())
                    elif operation.[0].CompareTo("Login") = 0 then
                        registrationHandlingActor <! Login(operation.[1],mailbox.Sender())
                    elif operation.[0].CompareTo("Logout") = 0 then
                        registrationHandlingActor <! Logout(operation.[1],mailbox.Sender())
                    elif operation.[0].CompareTo("Tweet") = 0 then
                        tweetHandlerActor <! AddTweet(operation.[1],string request_id,operation.[2],mailbox.Sender())
                    elif operation.[0].CompareTo("ReTweet") = 0 then
                        tweetHandlerActor <! ReTweet(operation.[1],mailbox.Sender())
                    elif operation.[0].CompareTo("GetTweets") = 0 then
                        theMainHandlerActor <! GetTweets(operation.[1],mailbox.Sender())
                    elif operation.[0].CompareTo("Subscribe") = 0 then 
                        handlers.followerHAndlerActor <! Add(operation.[1],operation.[2],mailbox.Sender())
                    elif operation.[0].CompareTo("GetMentions") = 0 then
                        theMainHandlerActor <! GetTags(operation.[1],mailbox.Sender())
                    elif operation.[0].CompareTo("GetHashTags") = 0 then
                        theMainHandlerActor <! GetHashTags(operation.[1],operation.[2],mailbox.Sender())
                    
                    ticks <- ticks + timer.ElapsedTicks
                    if request_id%10000 = 0 then
                        printfn "Time taken for %i requests took %A milliseconds" request_id ((ticks|>float)/ticksPerMilliSecond)

                    return! loop() 
                }
                
            loop()

printfn "All actors spawned.. Server started"
system.WhenTerminated.Wait()