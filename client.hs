{-# LANGUAGE BangPatterns #-}

module Main (main) where

import Control.Concurrent (forkFinally, myThreadId)
import Control.Concurrent.MVar (MVar, modifyMVar_, newEmptyMVar, newMVar, putMVar, takeMVar)
import Control.Exception (bracket, bracketOnError, displayException)
import Control.Monad (forM, when)
import Data.Bits (shiftR, xor)
import Data.Char (isSpace)
import Data.Int (Int64)
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as BL
import Data.Time (defaultTimeLocale, formatTime, getCurrentTime)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import Network.Socket
import Network.Socket.ByteString (recv, sendAll)
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.IO (Handle, IOMode (WriteMode), hFlush, hPutStrLn, openFile, stderr)

import Text.Read (readMaybe)

data ClientLogger = ClientLogger
    { loggerClientId :: !Int
    , loggerHandle :: !(MVar Handle)
    }

stripBS :: B8.ByteString -> B8.ByteString
stripBS = reverseStrip . B8.dropWhile isSpace
  where
    reverseStrip bs =
        case B8.unsnoc bs of
            Nothing -> bs
            Just (rest, c)
                | isSpace c -> reverseStrip rest
                | otherwise -> bs

logTimestamp :: IO String
logTimestamp = do
    now <- getCurrentTime
    pure (formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S.%q UTC" now)

clientLog :: ClientLogger -> String -> IO ()
clientLog logger message = do
    timestamp <- logTimestamp
    tid <- myThreadId
    let line = "[" ++ timestamp ++ "] [Client " ++ show (loggerClientId logger) ++ "][Thread " ++ show tid ++ "] " ++ message
    modifyMVar_ (loggerHandle logger) $ \handle -> do
        hPutStrLn handle line
        hFlush handle
        pure handle

mainLog :: ClientLogger -> String -> IO ()
mainLog logger message = do
    timestamp <- logTimestamp
    let line = "[" ++ timestamp ++ "] [Client " ++ show (loggerClientId logger) ++ "][MAIN] " ++ message
    modifyMVar_ (loggerHandle logger) $ \handle -> do
        hPutStrLn handle line
        hFlush handle
        pure handle

initClientLogger :: Int -> IO ClientLogger
initClientLogger clientId = do
    let fileName = "client" ++ show clientId ++ ".log"
    handle <- openFile fileName WriteMode
    handleVar <- newMVar handle
    pure (ClientLogger clientId handleVar)

data Config = Config
    { configClientId :: !Int
    , configWorkers :: !Int
    , configNumbers :: !Int
    , configHost :: String
    , configPort :: String
    }

data WorkerResult = WorkerResult
    { workerLocalSum :: !Int64
    , workerServerSum :: !(Maybe Int64)
    }

main :: IO ()
main = do
    args <- getArgs
    case parseConfig args of
        Nothing -> do
            hPutStrLn stderr "usage: client <clientId> [workers] [numbers]"
            exitFailure
        Just config -> runClient config

parseConfig :: [String] -> Maybe Config
parseConfig args =
    case args of
        [clientIdText] ->
            buildConfig clientIdText (Just "4") (Just "1000000")
        [clientIdText, workersText] ->
            buildConfig clientIdText (Just workersText) (Just "1000000")
        [clientIdText, workersText, numbersText] ->
            buildConfig clientIdText (Just workersText) (Just numbersText)
        _ -> Nothing
  where
    buildConfig clientIdText workersText numbersText = do
        clientId <- readMaybe clientIdText
        workers <- readMaybe =<< workersText
        numbers <- readMaybe =<< numbersText
        if clientId > 0 && workers > 0 && numbers > 0
            then Just (Config clientId workers numbers "127.0.0.1" "8081")
            else Nothing

runClient :: Config -> IO ()
runClient config =
    withSocketsDo $ do
        logger <- initClientLogger (configClientId config)
        mainLog logger $
            "Client "
                ++ show (configClientId config)
                ++ " Ready | workers="
                ++ show (configWorkers config)
                ++ " | numbers="
                ++ show (configNumbers config)
                ++ " | target="
                ++ configHost config
                ++ ":"
                ++ configPort config
        workerVars <- forM [0 .. configWorkers config - 1] $ \workerId -> do
            resultVar <- newEmptyMVar
            _ <- forkFinally
                (runWorker logger config workerId)
                (putMVar resultVar)
            pure resultVar
        results <- mapM takeMVar workerVars
        case sequence results of
            Left exception -> do
                hPutStrLn stderr $ "worker failed: " ++ displayException exception
                mainLog logger $ "Worker failed | exception=" ++ displayException exception
                exitFailure
            Right workerResults -> reportResult logger config workerResults

runWorker :: ClientLogger -> Config -> Int -> IO WorkerResult
runWorker logger config workerId = do
    let workerLabel = "[W" ++ show workerId ++ "]"
    clientLog logger $ workerLabel ++ " Starting worker"
    seed <- makeSeed config workerId
    bracket (openConnection logger config workerId) close $ \connection -> do
        let count = workerCount config workerId
        clientLog logger $
            workerLabel ++ " Sending INIT | clientId="
                ++ show (configClientId config)
                ++ " workerId="
                ++ show workerId
                ++ " workerCount="
                ++ show (configWorkers config)
                ++ " numberCount="
                ++ show count
        sendAll connection (encodeInit config workerId count)
        clientLog logger $
            workerLabel ++ " Sending numbers | count=" ++ show count ++ " | seed=SplitMix64"
        localSum <- sendNumbers logger config workerId connection count seed
        clientLog logger $ workerLabel ++ " Sending END"
        sendAll connection (B8.pack "END\n")
        serverSum <-
            if workerId == 0
                then do
                    clientLog logger $ workerLabel ++ " Waiting for RESULT from server"
                    result <- receiveResult connection
                    clientLog logger $ workerLabel ++ " Received RESULT | serverSum=" ++ show result
                    pure (Just result)
                else pure Nothing
        clientLog logger $ workerLabel ++ " Worker finished | localSum=" ++ show localSum
        pure (WorkerResult localSum serverSum)
  where
    workerCount cfg w =
        let total = configNumbers cfg
            workers = configWorkers cfg
        in total `div` workers + if w < total `mod` workers then 1 else 0

makeSeed :: Config -> Int -> IO Word64
makeSeed config workerId = do
    timestamp <- getMonotonicTimeNSec
    let clientPart = fromIntegral (configClientId config) * 0x9E3779B97F4A7C15
        workerPart = fromIntegral (workerId + 1) * 0xBF58476D1CE4E5B9
    pure (timestamp + clientPart + workerPart)

openConnection :: ClientLogger -> Config -> Int -> IO Socket
openConnection logger config workerId = do
    let workerLabel = "[W" ++ show workerId ++ "]"
    clientLog logger $
        workerLabel ++ " Establishing connection to "
            ++ configHost config
            ++ ":"
            ++ configPort config
    address : _ <- getAddrInfo
        (Just defaultHints { addrSocketType = Stream })
        (Just (configHost config))
        (Just (configPort config))
    bracketOnError
        (socket (addrFamily address) Stream (addrProtocol address))
        close
        (\connection -> do
            connect connection (addrAddress address)
            clientLog logger $
                workerLabel ++ " Connection established | localSocket="
                    ++ show connection
                    ++ " | peer="
                    ++ show (addrAddress address)
            pure connection)

encodeInit :: Config -> Int -> Int -> B8.ByteString
encodeInit config workerId count =
    B8.pack $
        "INIT "
            ++ show (configClientId config)
            ++ " "
            ++ show workerId
            ++ " "
            ++ show (configWorkers config)
            ++ " "
            ++ show count
            ++ "\n"

sendNumbers :: ClientLogger -> Config -> Int -> Socket -> Int -> Word64 -> IO Int64
sendNumbers logger _ workerId connection totalNumbers initialSeed =
    go totalNumbers initialSeed 0
  where
    go 0 !_ !total = pure total
    go remaining !seed !total = do
        let chunkSize = min 10000 remaining
            (payload, nextSeed, chunkSum) = buildChunk chunkSize seed
        sendAll connection (BL.toStrict (BB.toLazyByteString payload))
        let newTotal = total + chunkSum
        when (remaining == chunkSize || (totalNumbers - remaining) `mod` 10000 == 0) $
            clientLog logger $
                "[W" ++ show workerId ++ "] Sent batch | numbers="
                    ++ show chunkSize
                    ++ " | localSumSoFar="
                    ++ show newTotal
        go (remaining - chunkSize) nextSeed newTotal

buildChunk :: Int -> Word64 -> (BB.Builder, Word64, Int64)
buildChunk count initialSeed =
    go count initialSeed mempty 0
  where
    go 0 !seed !builder !total = (builder, seed, total)
    go remaining !seed !builder !total =
        let (nextSeed, value) = splitMix seed
            numberValue = fromIntegral (value `mod` 1001) :: Int64
            numberInt = fromIntegral numberValue :: Int
        in go
            (remaining - 1)
            nextSeed
            (builder <> BB.intDec numberInt <> BB.char7 '\n')
            (total + numberValue)

splitMix :: Word64 -> (Word64, Word64)
splitMix state =
    let nextState = state + 0x9E3779B97F4A7C15
        mixed0 = nextState
        mixed1 = (mixed0 `xor` (mixed0 `shiftR` 30)) * 0xBF58476D1CE4E5B9
        mixed2 = (mixed1 `xor` (mixed1 `shiftR` 27)) * 0x94D049BB133111EB
    in (nextState, mixed2 `xor` (mixed2 `shiftR` 31))

receiveResult :: Socket -> IO Int64
receiveResult connection = do
    message <- receiveLine connection mempty
    case message of
        Nothing -> ioError (userError "server closed the connection without a result")
        Just line ->
            case B8.stripPrefix (B8.pack "RESULT ") (stripBS line) of
                Just valueText ->
                    case readMaybe (B8.unpack (stripBS valueText)) of
                        Just value -> pure value
                        Nothing -> ioError (userError "invalid result received from server")
                Nothing -> ioError (userError "unexpected response from server")

receiveLine :: Socket -> B8.ByteString -> IO (Maybe B8.ByteString)
receiveLine connection buffer =
    case B8.elemIndex '\n' buffer of
        Just index ->
            pure (Just (B8.take index buffer))
        Nothing -> do
            chunk <- recv connection 4096
            if B8.null chunk
                then pure Nothing
                else receiveLine connection (buffer <> chunk)

reportResult :: ClientLogger -> Config -> [WorkerResult] -> IO ()
reportResult logger config results =
    case results of
        [] -> do
            hPutStrLn stderr "no worker results"
            mainLog logger "No worker results"
            exitFailure
        firstResult : _ -> do
            let localSum = sum (map workerLocalSum results)
            case workerServerSum firstResult of
                Nothing -> do
                    hPutStrLn stderr "missing server result"
                    mainLog logger "Missing server result"
                    exitFailure
                Just serverSum -> do
                    let status = if localSum == serverSum then "OK" else "MISMATCH"
                    putStrLn $
                        "client "
                            ++ show (configClientId config)
                            ++ " workers="
                            ++ show (configWorkers config)
                            ++ " numbers="
                            ++ show (configNumbers config)
                            ++ " local_sum="
                            ++ show localSum
                            ++ " server_sum="
                            ++ show serverSum
                            ++ " status="
                            ++ status
                    mainLog logger $
                        "All workers finished | localSumTotal="
                            ++ show localSum
                            ++ " | serverSum="
                            ++ show serverSum
                            ++ " | status="
                            ++ status
                    if localSum == serverSum
                        then pure ()
                        else exitFailure
