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
  , createInbox
  ) where

import           Control.Concurrent (threadDelay)
import           Control.Monad
-- import           Data.Text          (Text)
import           NQE
import           UnliftIO

import           CoinMetrics.BlockChain
-- import           CoinMetrics.BlockChain.All

-- logStrLn :: String -> IO ()
-- logStrLn str = withMVar logStrMVar $ \_ -> hPutStrLn stderr str

-- logPrint :: Show a => a -> IO ()
-- logPrint = logStrLn . show

data GlobalTopMsg = GetGlobalTop (Listen BlockHeight) 
                  | SetGlobalTop BlockHeight

data NodeMsg = GetLocalTop (Listen BlockHeight) 
             | SetLocalTop BlockHeight

data FetchBlockMsg = Fetch (BlockHeight, BlockHeight)

data PersistBlockMsg b = Persist (BlockHeight, BlockHeight, b)

blocker :: IO ()
blocker = forever $ threadDelay 1000000000

topChainExplorer ::
     BlockChain a
  => BlockHeight
  -> Mailbox NodeMsg
  -> Mailbox GlobalTopMsg
  -> a
  -> Int
  -> IO ()
topChainExplorer endBlock nodeM globalM blockchain i = do
  print $ "start topChainExplorer: " <> show i
  forever $ do
    -- print $ "bucle topchainexplorer: " <> show i
    eBlockIndex <- try $ getCurrentBlockHeight blockchain
    case eBlockIndex of
      Right current -> do
        -- print "TOPCE: going to obtain BH"
        atomically $ do
          let limit = current + endBlock + 1
          SetLocalTop limit `sendSTM` nodeM
          SetGlobalTop limit `sendSTM` globalM
        -- print "TOPCE: send shit"
      Left (SomeException err) -> print err
    threadDelay 10000000

nodeManager ::
     BlockChain a
  => BlockHeight
  -> Mailbox GlobalTopMsg
  -> (Inbox NodeMsg, a, Int)
  -> IO ()
nodeManager endBlock globalM (nodeI, blockchain, i) = do
  print $ "node manager start: " <> show i
  upperLimit <- newTVarIO 0
  let nodeM = inboxToMailbox nodeI
  _ <- async $ topChainExplorer endBlock nodeM globalM blockchain i
  forever $ do
    -- print $ "bucle nodemanager: " <> show i
    msg <- receive nodeI
    case msg of
      GetLocalTop reply -> atomically $ readTVar upperLimit >>= reply
      SetLocalTop found -> atomically $ do
        limit <- readTVar upperLimit
        when (found > limit) $ writeTVar upperLimit found


globalTopManager :: Inbox GlobalTopMsg -> IO ()
globalTopManager inbox = do
  print "start topChainManager"
  upperLimit <- newTVarIO 0
  forever $ do
    -- print "bucle topChainManager"
    msg <- receive inbox
    case msg of
      GetGlobalTop reply -> atomically $ readTVar upperLimit >>= reply
      SetGlobalTop found -> atomically $ do
        limit <- readTVar upperLimit
        when (found > limit) $ writeTVar upperLimit found

nextBlockExplorer :: BlockHeight
                  -> BlockHeight
                  -> Mailbox GlobalTopMsg
                  -> Mailbox FetchBlockMsg
                  -> IO ()
nextBlockExplorer beginBlock endBlock globalM fetchM = do
  print "start nextBlockExplorer"
  currentBox <- newTVarIO beginBlock
  forever $ do
    -- print "bucle nextBlockExplorer"
    top <- GetGlobalTop `query` globalM
    atomically $ do
      current <- readTVar currentBox
      when (current < top) $ do
        modifyTVar currentBox (+1)
        Fetch (current, current + 1) `sendSTM` fetchM

fetchWorker ::
     BlockChain a
  => Inbox FetchBlockMsg
  -> Mailbox (PersistBlockMsg (Block a))
  -> (Mailbox NodeMsg, a, Int)
  -> IO ()
fetchWorker inbox persistM (localTopM, blockchain, i) = do
  print $ "start worker: " <> show i
  forever $ do
    localTop <- GetLocalTop `query` localTopM
    blockM <- receiveMatchS timeOut inbox (selectIndex localTop)
    case blockM of
      Just (current, next) -> do
        eitherBlock <- try $ getBlockByHeight blockchain current
        case eitherBlock of
          Right block -> Persist (current, next, block) `send` persistM
          Left (SomeException err) -> do
            print err  -- somebody else will try again
            Fetch (current, next) `send` inbox
            -- there was this logic ignoreErrorsEnabled that I dont quite follow
      Nothing -> return ()
  where
    timeOut = 2 -- 2 secs
    selectIndex top (Fetch (current, next)) =
      if current <= top then Just (current, next) else Nothing

persistenceActor :: HasBlockHeader a => BlockHeight -> Inbox (PersistBlockMsg a) -> IO ()
persistenceActor firstBlock inbox = do
  nextToSaveT <- newTVarIO firstBlock
  print "start persistence Actor"
  forever $ do
    block <- atomically $ do
      nextToSave <- readTVar nextToSaveT
      (_, next, b) <- receiveMatchSTM inbox (selectBlock nextToSave)
      writeTVar nextToSaveT next
      return b

    let header = getBlockHeader block
    putStr "Save: "
    print (bh_height header)   
  where
    selectBlock wanted (Persist b@(found, _, _)) =
      if found == wanted then Just b else Nothing

createInbox :: BlockChain a => (Int, a) -> IO (Inbox b, a, Int)
createInbox (i, blockchain) = do
  inbox <- newInbox
  return (inbox, blockchain, i)
