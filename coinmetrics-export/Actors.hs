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
import           Control.Concurrent.STM (retry)

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

-- Constant pulling of BlockHeigh
-- Notify the global BH tracker
-- Notify the local BH tracker
topChainExplorer ::
     BlockChain a
  => BlockHeight
  -> Mailbox GlobalTopMsg
  -> (Mailbox NodeMsg, a)
  -> IO ()
topChainExplorer endBlock globalM (nodeM, blockchain) = do
  let lowerLimit = if endBlock < 0 then endBlock + 1 else 0
  forever $ do
    eBlockIndex <- try $ getCurrentBlockHeight blockchain
    case eBlockIndex of
      Right current -> do
        let limit = current + lowerLimit
        atomically $ do
          SetLocalTop limit `sendSTM` nodeM
          SetGlobalTop limit `sendSTM` globalM
      Left (SomeException err) -> print err
    threadDelay 10000000

nodeManager :: Inbox NodeMsg -> IO ()
nodeManager nodeI = do
  upperLimit <- newTVarIO (-1)
  forever $ do
    msg <- receive nodeI
    case msg of
      GetLocalTop reply -> atomically $ do
        l <- readTVar upperLimit
        if l < 0
          then GetLocalTop reply `sendSTM` nodeI
          else reply l
      SetLocalTop found ->
        atomically $ do
          limit <- readTVar upperLimit
          when (found > limit) $ writeTVar upperLimit found

-- This is the global BH tracker. Next-block-explorer uses that 
-- to know the limits no matter what node you could be connected
globalTopManager :: Inbox GlobalTopMsg -> IO ()
globalTopManager inbox = do
  upperLimit <- newTVarIO 0
  forever $ do
    msg <- receive inbox
    case msg of
      GetGlobalTop reply -> atomically $ readTVar upperLimit >>= reply
      SetGlobalTop found -> atomically $ do
        limit <- readTVar upperLimit
        when (found > limit) $ writeTVar upperLimit found

-- This is in charge to create the list of block to download
-- That might depend on the mode the program is running
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
      blocker -- I should have a better supervisor policy

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

-- These are the actual workers downloading the blocks
fetchWorker ::
     BlockChain a
  => Inbox FetchBlockMsg
  -> Mailbox (PersistBlockMsg (Block a))
  -> TVar Int
  -> Int
  -> (Mailbox NodeMsg, a)
  -> IO ()
fetchWorker inbox persistM unsavedT buffer (localTopM, blockchain) =
  forever $ do
    localTop <- GetLocalTop `query` localTopM
    (current, next) <- atomically $ do
      p <- receiveMatchSTM inbox (selectIndex localTop)
      modifyTVar unsavedT (+ 1)
      unsaved <- readTVar unsavedT
      when (unsaved > buffer) retry
      return p

    eitherBlock <- try $ getBlockByHeight blockchain current
    case eitherBlock of
      Right block -> Persist current next block `send` persistM
      Left (SomeException err) -> do
        print err  -- somebody else will try again
        Fetch current next `send` inbox
  where
    selectIndex top (Fetch current next) =
      if current <= top then Just (current, next) else Nothing

-- This actor is in charge to write the fetched blocks to the database
-- it keeps waiting for more blocks unless the next block is -1
persistenceActor ::
     HasBlockHeader a
  => Inbox (PersistBlockMsg a)
  -> ([a] -> IO ())
  -> TVar Int
  -> IO ()
persistenceActor inbox writeOutput unsavedT = do
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
      (current, next, block) <- atomically $ do
        modifyTVar unsavedT (subtract 1)
        receiveMatchSTM inbox (selectBlock i)
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

actorApp ::
     BlockChain a
  => [a]
  -> BlockHeight
  -> BlockHeight
  -> Maybe String
  -> Int
  -> ([Block a] -> IO ())
  -> Supervisor
  -> IO ()
actorApp blockchains beginBlock endBlock fileB threadsC writeOuts sup
 = do
  unsavedT <- newTVarIO 0
  let nodesCount = length blockchains
  let bound = fromIntegral (nodesCount * threadsC * 2)
  topInbox <- newBoundedInbox bound
  fetchInbox <- newBoundedInbox bound
  persistInbox <- newBoundedInbox bound
  blockchainInboxes <- mapM assocInbox blockchains
  
  let persistMailbox = inboxToMailbox persistInbox
  let topMailbox = inboxToMailbox topInbox
  let fetchMailbox = inboxToMailbox fetchInbox
  let blockchainMailboxes =
        map (\(i, b) -> (inboxToMailbox i, b)) blockchainInboxes

  _gtm <- addChild sup $ globalTopManager topInbox
  _pa <- addChild sup $ persistenceActor persistInbox writeOuts unsavedT
  let explorers = map (topChainExplorer endBlock topMailbox) blockchainMailboxes
  _es <- mapM (addChild sup) explorers
  let localManagers = map (nodeManager . fst) blockchainInboxes
  _lms <- mapM (addChild sup) localManagers
  _nbe <- addChild sup
    $ nextBlockExplorer
        beginBlock
        endBlock
        fileB
        topMailbox
        fetchMailbox
        persistMailbox

  let worker = fetchWorker fetchInbox persistMailbox unsavedT (fromIntegral bound)
  let workers = join $ map (replicate threadsC . worker) blockchainMailboxes
  _ws <- mapM (addChild sup) workers
  wait _pa -- wait for persistence actor
