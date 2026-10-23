{-# LANGUAGE BangPatterns #-}

module Main (main) where

import Control.Concurrent (forkFinally, myThreadId)
import Control.Concurrent.MVar (MVar, modifyMVar_, newMVar)
import Control.Concurrent.STM
import Control.Exception (SomeException, bracket, displayException)
import Control.Monad (forever, unless, void, when)
import Data.Char (isSpace)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as B8
import Data.Int (Int64)
import qualified Data.Map.Strict as Map
import Data.Time (defaultTimeLocale, formatTime, getCurrentTime)
import Data.Time.Clock (UTCTime, diffUTCTime)
import Network.Socket
import Network.Socket.ByteString (recv, sendAll)
import Numeric (showFFloat)
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.IO
    ( BufferMode (LineBuffering)
    , Handle
    , IOMode (WriteMode)
    , hFlush
    , hPutStrLn
    , hSetBuffering
    , openFile
    , stderr
    , stdout
    )
import Text.Read (readMaybe)

data ServerLogger = ServerLogger
    { serverLogHandle :: !(MVar Handle)
    }

logTimestamp :: IO String
logTimestamp = do
    now <- getCurrentTime
    pure (formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S.%q UTC" now)

serverLog :: ServerLogger -> String -> IO ()
serverLog logger message = do
    timestamp <- logTimestamp
    tid <- myThreadId
    let line = "[" ++ timestamp ++ "] [SERVER][Thread " ++ show tid ++ "] " ++ message
    modifyMVar_ (serverLogHandle logger) $ \handle -> do
        hPutStrLn handle line
        hFlush handle
        pure handle

serverLogMain :: ServerLogger -> String -> IO ()
serverLogMain logger message = do
    timestamp <- logTimestamp
    let line = "[" ++ timestamp ++ "] [SERVER][MAIN] " ++ message
    modifyMVar_ (serverLogHandle logger) $ \handle -> do
        hPutStrLn handle line
        hFlush handle
        pure handle

initServerLogger :: IO ServerLogger
initServerLogger = do
    handle <- openFile "server.log" WriteMode
    handleVar <- newMVar handle
    pure (ServerLogger handleVar)

stripBS :: B8.ByteString -> B8.ByteString
stripBS = reverseStrip . B8.dropWhile isSpace
  where
    reverseStrip bs =
        case B8.unsnoc bs of
            Nothing -> bs
            Just (rest, c)
                | isSpace c -> reverseStrip rest
                | otherwise -> bs

data Session = Session
    { sessionExpectedWorkers :: !Int
    , sessionRemainingWorkers :: !Int
    , sessionPartialTotal :: !Int64
    , sessionStartedAt :: !UTCTime
    }

data ServerState = ServerState
    { stateSessions :: !(TVar (Map.Map Int Session))
    , stateResults :: !(TVar (Map.Map Int Int64))
    , stateCompletedClients :: !(TVar Int)
    , stateFirstStart :: !(TVar (Maybe UTCTime))
    , stateExpectedClients :: !Int
    , stateLogger :: !ServerLogger
    }

main :: IO ()
main = do
    hSetBuffering stdout LineBuffering
    args <- getArgs
    expectedClients <-
        case args of
            [] -> pure 0
            [value] ->
                case readMaybe value of
                    Just parsed | parsed >= 0 -> pure parsed
                    _ -> usage
            _ -> usage
    runServer expectedClients

usage :: IO a
usage = do
    hPutStrLn stderr "usage: server [expectedClients]"
    exitFailure

runServer :: Int -> IO ()
runServer expectedClients =
    withSocketsDo $ do
        logger <- initServerLogger
        sessions <- newTVarIO Map.empty
        results <- newTVarIO Map.empty
        completedClients <- newTVarIO 0
        firstStart <- newTVarIO Nothing
        let state = ServerState
                sessions
                results
                completedClients
                firstStart
                expectedClients
                logger
        bracket
            (openServer logger expectedClients)
            close
            (acceptConnections state)

openServer :: ServerLogger -> Int -> IO Socket
openServer logger expectedClients = do
    let hints = defaultHints { addrSocketType = Stream }
    address : _ <- getAddrInfo
        (Just hints)
        (Just "127.0.0.1")
        (Just "8081")
    serverSocket <- socket
        (addrFamily address)
        (addrSocketType address)
        (addrProtocol address)
    setSocketOption serverSocket ReuseAddr 1
    bind serverSocket (addrAddress address)
    listen serverSocket 128
    putStrLn "READY server listening on 127.0.0.1:8081"
    hFlush stdout
    serverLogMain logger $
        "Ready | listening on 127.0.0.1:8081 | expectedClients="
            ++ show expectedClients
            ++ " | backlog=128"
    pure serverSocket

acceptConnections :: ServerState -> Socket -> IO ()
acceptConnections state serverSocket = forever $ do
    (clientSocket, peerAddress) <- accept serverSocket
    putStrLn $ "connected " ++ show peerAddress
    hFlush stdout
    serverLogMain (stateLogger state) $
        "Client connected | peer=" ++ show peerAddress ++ " | socket=" ++ show clientSocket
    void $ forkFinally
        (handleClient state clientSocket peerAddress)
        (finishClient (stateLogger state) clientSocket peerAddress)

finishClient :: ServerLogger -> Socket -> SockAddr -> Either SomeException () -> IO ()
finishClient logger clientSocket peerAddress result = do
    close clientSocket
    serverLogMain logger $ "Closing connection to " ++ show peerAddress ++ " | socket=" ++ show clientSocket
    case result of
        Left exception ->
            hPutStrLn stderr $ "client handler failed: " ++ displayException exception
        Right () -> pure ()

handleClient :: ServerState -> Socket -> SockAddr -> IO ()
handleClient state clientSocket peerAddress = do
    let logger = stateLogger state
    serverLog logger $
        "Waiting for INIT from " ++ show peerAddress ++ " | socket=" ++ show clientSocket
    header <- receiveLine clientSocket BS.empty
    case header of
        Nothing -> ioError (userError "connection closed before INIT")
        Just (headerLine, buffered) ->
            case parseInit headerLine of
                Nothing -> ioError (userError "invalid INIT header")
                Just (clientId, workerId, workerCount, numberCount) -> do
                    serverLog logger $
                        "[C"
                            ++ show clientId
                            ++ "][W"
                            ++ show workerId
                            ++ "] Received INIT | clientId="
                            ++ show clientId
                            ++ " workerId="
                            ++ show workerId
                            ++ " workerCount="
                            ++ show workerCount
                            ++ " numberCount="
                            ++ show numberCount
                            ++ " | peer="
                            ++ show peerAddress
                    now <- getCurrentTime
                    validSession <- atomically $ do
                        sessions <- readTVar (stateSessions state)
                        resultsMap <- readTVar (stateResults state)
                        case Map.lookup clientId sessions of
                            Nothing -> do
                                let session = Session workerCount workerCount 0 now
                                writeTVar
                                    (stateSessions state)
                                    (Map.insert clientId session sessions)
                                writeTVar
                                    (stateResults state)
                                    (Map.delete clientId resultsMap)
                                pure True
                            Just session ->
                                pure (sessionExpectedWorkers session == workerCount)
                    unless validSession $
                        ioError (userError "worker count differs from existing session")
                    atomically $
                        modifyTVar' (stateFirstStart state) $ \current ->
                            case current of
                                Nothing -> Just now
                                Just value -> Just value
                    serverLog logger $
                        "[C"
                            ++ show clientId
                            ++ "][W"
                            ++ show workerId
                            ++ "] Receiving numbers from stream"
                    (partialTotal, receivedCount) <- sumStream state clientId workerId clientSocket buffered
                    unless (receivedCount == numberCount) $
                        ioError (userError "received number count does not match INIT header")
                    serverLog logger $
                        "[C"
                            ++ show clientId
                            ++ "][W"
                            ++ show workerId
                            ++ "] Received END | numbersReceived="
                            ++ show receivedCount
                            ++ " | partialSum="
                            ++ show partialTotal
                    void $ registerPartial state clientId partialTotal
                    when (workerId == 0) $ do
                        total <- waitForResult state clientId
                        serverLog logger $
                            "[C"
                                ++ show clientId
                                ++ "][W"
                                ++ show workerId
                                ++ "] Sending RESULT | totalSum="
                                ++ show total
                                ++ " | to peer="
                                ++ show peerAddress
                        sendAll clientSocket (B8.pack ("RESULT " ++ show total ++ "\n"))

parseInit :: B8.ByteString -> Maybe (Int, Int, Int, Int)
parseInit line =
    case words (B8.unpack line) of
        ["INIT", clientIdText, workerIdText, workerCountText, numberCountText] -> do
            clientId <- readMaybe clientIdText
            workerId <- readMaybe workerIdText
            workerCount <- readMaybe workerCountText
            numberCount <- readMaybe numberCountText
            if clientId > 0
                && workerCount > 0
                && workerId >= 0
                && workerId < workerCount
                && numberCount >= 0
                then Just (clientId, workerId, workerCount, numberCount)
                else Nothing
        _ -> Nothing

sumStream :: ServerState -> Int -> Int -> Socket -> BS.ByteString -> IO (Int64, Int)
sumStream state clientId workerId clientSocket = go 0 0
  where
    logger = stateLogger state
    go !total !count buffer =
        case BS.elemIndex 10 buffer of
            Just index ->
                let line = BS.take index buffer
                    rest = BS.drop (index + 1) buffer
                in if stripBS line == B8.pack "END"
                    then pure (total, count)
                    else case parseNumber line of
                        Nothing -> ioError (userError "invalid number received")
                        Just number ->
                            let newTotal = total + number
                                newCount = count + 1
                            in do
                                when (newCount `mod` 10000 == 0) $
                                    serverLog logger $
                                        "[C"
                                            ++ show clientId
                                            ++ "][W"
                                            ++ show workerId
                                            ++ "] Received batch | count="
                                            ++ show newCount
                                            ++ " | partialSum="
                                            ++ show newTotal
                                go newTotal newCount rest
            Nothing -> do
                chunk <- recv clientSocket 65536
                if BS.null chunk
                    then ioError (userError "connection closed before END")
                    else go total count (buffer <> chunk)

parseNumber :: B8.ByteString -> Maybe Int64
parseNumber line =
    case B8.readInt (stripBS line) of
        Just (value, rest)
            | B8.null rest && value >= 0 && value <= 1000 ->
                Just (fromIntegral value)
        _ -> Nothing

registerPartial :: ServerState -> Int -> Int64 -> IO (Maybe Int64)
registerPartial state clientId partialTotal = do
    now <- getCurrentTime
    let logger = stateLogger state
    completed <- atomically $ do
        sessions <- readTVar (stateSessions state)
        case Map.lookup clientId sessions of
            Nothing -> pure Nothing
            Just session -> do
                let remaining = sessionRemainingWorkers session - 1
                    total = sessionPartialTotal session + partialTotal
                    updated = session
                        { sessionRemainingWorkers = remaining
                        , sessionPartialTotal = total
                        }
                if remaining == 0
                    then do
                        modifyTVar'
                            (stateSessions state)
                            (Map.delete clientId)
                        modifyTVar'
                            (stateResults state)
                            (Map.insert clientId total)
                        modifyTVar' (stateCompletedClients state) (+ 1)
                        pure (Just (total, sessionStartedAt session, sessionRemainingWorkers session))
                    else do
                        writeTVar
                            (stateSessions state)
                            (Map.insert clientId updated sessions)
                        pure (Just (total, sessionStartedAt session, remaining))
    case completed of
        Nothing -> pure Nothing
        Just (total, startedAt, remainingWorkers) -> do
            serverLog logger $
                "[C"
                    ++ show clientId
                    ++ "] Partial sum registered | remainingWorkers="
                    ++ show remainingWorkers
                    ++ " | clientPartialSoFar="
                    ++ show total
            done <- readTVarIO (stateCompletedClients state)
            let elapsedMs = elapsedMilliseconds startedAt now
            putStrLn $
                "client "
                    ++ show clientId
                    ++ " total="
                    ++ show total
                    ++ " elapsed_ms="
                    ++ formatMilliseconds elapsedMs
            hFlush stdout
            serverLog logger $
                "[C"
                    ++ show clientId
                    ++ "] Client total computed | totalSum="
                    ++ show total
                    ++ " | elapsed_ms="
                    ++ formatMilliseconds elapsedMs
            when (remainingWorkers == 0) $
                serverLog logger $
                    "[C" ++ show clientId ++ "] All workers completed for this client | totalSum=" ++ show total
            when
                ( stateExpectedClients state > 0
                    && done == stateExpectedClients state
                )
                (reportServerTotal state now)
            pure (Just total)

waitForResult :: ServerState -> Int -> IO Int64
waitForResult state clientId =
    atomically $ do
        results <- readTVar (stateResults state)
        case Map.lookup clientId results of
            Just total -> pure total
            Nothing -> retry

reportServerTotal :: ServerState -> UTCTime -> IO ()
reportServerTotal state finishedAt = do
    let logger = stateLogger state
    startedAt <- readTVarIO (stateFirstStart state)
    case startedAt of
        Nothing -> pure ()
        Just start -> do
            let elapsedMs = elapsedMilliseconds start finishedAt
            putStrLn $
                "SERVER_TOTAL_MS="
                    ++ formatMilliseconds elapsedMs
                    ++ " clients="
                    ++ show (stateExpectedClients state)
            hFlush stdout
            serverLogMain logger $
                "SERVER_TOTAL_MS="
                    ++ formatMilliseconds elapsedMs
                    ++ " clients="
                    ++ show (stateExpectedClients state)

elapsedMilliseconds :: UTCTime -> UTCTime -> Double
elapsedMilliseconds start end =
    realToFrac (diffUTCTime end start) * 1000

formatMilliseconds :: Double -> String
formatMilliseconds value = showFFloat (Just 3) value ""

receiveLine :: Socket -> BS.ByteString -> IO (Maybe (BS.ByteString, BS.ByteString))
receiveLine clientSocket buffer =
    case BS.elemIndex 10 buffer of
        Just index -> do
            let line = BS.take index buffer
                rest = BS.drop (index + 1) buffer
            pure (Just (line, rest))
        Nothing -> do
            chunk <- recv clientSocket 65536
            if BS.null chunk
                then pure Nothing
                else receiveLine clientSocket (buffer <> chunk)
