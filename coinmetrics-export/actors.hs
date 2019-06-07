{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}

module Actors
  ( blocker
  , nodeManager
  , globalTopManager
  , nextBlockExplorer
  , fetchWorker
  , topChainExplorer
  , persistenceActor
  , GlobalTopMsg (..)
  , FetchBlockMsg (..)
  , NodeMsg (..)
  , PersistBlockMsg (..)
  , assocInbox
  , actorApp
  ) where

import           Control.Concurrent (threadDelay)
import           Control.Monad
import           NQE
import           UnliftIO
import           System.IO.Unsafe
import qualified Data.Text.Lazy         as TL
import qualified Data.Text.Lazy.IO      as TL
import           CoinMetrics.BlockChain

-- logStrLn :: String -> IO ()
-- logStrLn str = withMVar logStrMVar $ \_ -> hPutStrLn stderr str

-- logPrint :: Show a => a -> IO ()
-- logPrint = logStrLn . show

data GlobalTopMsg = GetGlobalTop (Listen BlockHeight) 
                  | SetGlobalTop BlockHeight

data NodeMsg = GetLocalTop (Listen BlockHeight) 
             | SetLocalTop BlockHeight

data FetchBlockMsg = Fetch BlockHeight BlockHeight

data PersistBlockMsg b = Persist BlockHeight BlockHeight b
                       | Begin BlockHeight

blocker :: IO ()
blocker = forever $ threadDelay 1000000000

-- This actor is in charge to pull a single bitcoin node
-- Notify the global BH tracker
-- Notify its parent, the local BH tracker
topChainExplorer ::
     BlockChain a
  => BlockHeight
  -> Mailbox NodeMsg
  -> Mailbox GlobalTopMsg
  -> a
  -> IO ()
topChainExplorer endBlock nodeM globalM blockchain = do
  let lowerLimit = if endBlock < 0 then endBlock + 1 else 0
  forever $ do
    eBlockIndex <- try $ getCurrentBlockHeight blockchain
    case eBlockIndex of
      Right current ->
        atomically $ do
          let limit = current + lowerLimit
          SetLocalTop limit `sendSTM` nodeM
          SetGlobalTop limit `sendSTM` globalM
      Left (SomeException err) -> print err
    threadDelay 10000000

-- This actor will spawn a topChainExplorer that tracks the local BH
-- we spawn a node manager per crypto node we talk
-- it will be in charge to answer the local BH of the node he is connected 
nodeManager ::
     BlockChain a
  => BlockHeight
  -> Mailbox GlobalTopMsg
  -> (Inbox NodeMsg, a)
  -> IO ()
nodeManager endBlock globalM (nodeI, blockchain) = do
  upperLimit <- newTVarIO 0
  let nodeM = inboxToMailbox nodeI
  _ <- async $ topChainExplorer endBlock nodeM globalM blockchain
  forever $ do
    msg <- receive nodeI
    case msg of
      GetLocalTop reply -> atomically $ readTVar upperLimit >>= reply
      SetLocalTop found -> atomically $ do
        limit <- readTVar upperLimit
        when (found > limit) $ writeTVar upperLimit found


-- This actor get notified by all the explorers connected to all nodes and 
-- is tracking the maximum block heigh of all of them
globalTopManager :: Inbox GlobalTopMsg -> IO ()
globalTopManager inbox = do
  print "hello"
  upperLimit <- newTVarIO 0
  forever $ do
    msg <- receive inbox
    case msg of
      GetGlobalTop reply -> atomically $ readTVar upperLimit >>= reply
      SetGlobalTop found -> atomically $ do
        limit <- readTVar upperLimit
        when (found > limit) $ writeTVar upperLimit found

-- This actor is in charge to generate the list of events with all the blocks
-- that need to be fetched.
nextBlockExplorer :: HasBlockHeader a 
  => BlockHeight
  -> BlockHeight
  -> Maybe String
  -> Mailbox GlobalTopMsg
  -> Mailbox FetchBlockMsg
  -> Mailbox (PersistBlockMsg a)
  -> IO ()
nextBlockExplorer beginBlock endBlock blocksFileM globalM fetchM persistM = do
  blocksM <- case blocksFileM of
    Just f -> withAsync (readBlockFile f) (fmap Just . wait)
    Nothing -> if endBlock >= 0 && endBlock > beginBlock
               then return $ Just [beginBlock..(endBlock - 1)]
               else return Nothing
  case blocksM of
    -- finite list of blocks to download
    Just blocks -> do
      Begin (head blocks) `send` persistM
      let pairs = zip blocks (tail blocks ++ [-1])
      (uncurry Fetch <$> pairs) `sendList` fetchM
      blocker

    -- never ending mode
    Nothing -> do
      Begin beginBlock `send` persistM
      currentBox <- newTVarIO beginBlock
      forever $ do
        top <- GetGlobalTop `query` globalM
        atomically $ do
          current <- readTVar currentBox
          when (current < top) $ do
            modifyTVar currentBox (+ 1)
            Fetch current (current + 1) `sendSTM` fetchM

-- We spawns lots of this actor to fetch blocks
fetchWorker ::
     BlockChain a
  => Inbox FetchBlockMsg
  -> Mailbox (PersistBlockMsg (Block a))
  -> (Mailbox NodeMsg, a)
  -> IO ()
fetchWorker inbox persistM (localTopM, blockchain) =
  forever $ do
    localTop <- GetLocalTop `query` localTopM
    blockM <- receiveMatchS timeOut inbox (selectIndex localTop)
    case blockM of
      Just (current, next) -> do
        eitherBlock <- try $ getBlockByHeight blockchain current
        case eitherBlock of
          Right block -> Persist current next block `send` persistM
          Left (SomeException err) -> do
            print err  -- somebody else will try again
            Fetch current next `send` inbox
            -- there was this logic ignoreErrorsEnabled that I dont quite follow
      Nothing -> return ()
  where
    timeOut = 4 -- 4 secs. This is needed in case you try to fetch a blockhigh non existend, like 0
    selectIndex top (Fetch current next) =
      if current <= top then Just (current, next) else Nothing

-- This actor is in charge to write the fetched blocks to the database
-- it keeps waiting for more blocks unless the next block is -1
persistenceActor ::
     HasBlockHeader a => Inbox (PersistBlockMsg a) -> ([a] -> IO ()) -> IO ()
persistenceActor inbox writeOutput = do
    beginBlock <- atomically $ receiveMatchSTM inbox selectBegin
    listToWrite beginBlock >>= writeOutput
    putStrLn "Sync complete"
  where
    selectBegin (Begin bh) = Just bh
    selectBegin _ = Nothing
    selectBlock wanted (Persist found next block) =
      if found == wanted then Just (found, next, block) else Nothing
    selectBlock _ _ = Nothing
    listToWrite (-1) = return [] -- when (next = -1) -> signal for endblock
    listToWrite i = unsafeInterleaveIO $ do
      (current, next, block) <- atomically $ receiveMatchSTM inbox (selectBlock i)
      when (current `rem` 100 == 0) $ print $ "synced up to " <> show current
      (block :) <$> listToWrite next

assocInbox :: BlockChain a => a -> IO (Inbox b, a)
assocInbox blockchain = do
  inbox <- newInbox
  return (inbox, blockchain)

readBlockFile :: String -> IO [BlockHeight]
readBlockFile file = map (read . TL.unpack) . TL.lines <$> TL.readFile file

sendList :: (MonadIO m, OutChan mbox) => [msg] -> mbox msg -> m ()
sendList xs mbox = mapM_ (`send` mbox) xs

actorApp :: Supervisor -> IO ()
actorApp sup = do
  topInbox <- newInbox
  mapM_ (addChild sup) $ [globalTopManager topInbox]
  blocker