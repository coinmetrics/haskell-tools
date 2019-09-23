{-# LANGUAGE LambdaCase, OverloadedStrings, ViewPatterns, TypeApplications #-}

module Main(main) where

import           Control.Monad
import           Data.Default
import           Data.String
import qualified Network.Connection                      as NC
import qualified Network.HTTP.Client                     as H
import qualified Network.HTTP.Client.TLS                 as H
import           NQE
import qualified Options.Applicative                     as O

import           Actors 
import           CoinMetrics.BlockChain
import           CoinMetrics.BlockChain.All
import           Options

main :: IO ()
main = do
  prepare
  run =<< O.execParser parser

run :: Options -> IO ()
run Options
  { options_command = command
  } = case command of

  OptionExportCommand
    { options_apis = apisByUser
    , options_blockchain = blockchainType
    , options_beginBlock = maybeBeginBlock
    , options_endBlock = _
    , options_blocksFile = _
    , options_continue = _
    , options_trace = trace
    , options_excludeUnaccountedActions = excludeUnaccountedActions
    , options_output = output
    , options_threadsCount = threadsCount
    , options_ignoreMissingBlocks = _
    } -> do
    -- get blockchain info by name
    SomeBlockChainInfo BlockChainInfo
      { bci_init = initBlockChain
      , bci_defaultApiUrls = defaultApiUrls
      , bci_defaultBeginBlock = defaultBeginBlock
      , bci_defaultEndBlock = _
      , bci_heightFieldName = heightFieldName
      , bci_hashFieldName = hashFieldName
      , bci_flattenSuffixes = flattenSuffixes
      , bci_flattenPack = flattenPack
      } <- maybe (fail "wrong blockchain type") return $ getSomeBlockChainInfo blockchainType

    let
      apis = if null apisByUser
        then map (\defaultApiUrl -> Api
          { api_apiUrl = defaultApiUrl
          , api_apiUrlUserName = ""
          , api_apiUrlPassword = ""
          , api_apiUrlInsecure = False
          }) defaultApiUrls
        else apisByUser
      apisCount = length apis
      httpManagerConnCount = (apisCount * threadsCount + 1) * 2

    -- init http managers
    httpSecureManager <- H.newTlsManagerWith H.tlsManagerSettings
      { H.managerConnCount = httpManagerConnCount
      }
    httpInsecureManager <- H.newTlsManagerWith (H.mkManagerSettings def
      { NC.settingDisableCertificateValidation = True
      } Nothing)
      { H.managerConnCount = httpManagerConnCount
      }

    -- init blockchains
    blockchains <- forM apis $ \Api
      { api_apiUrl = apiUrl
      , api_apiUrlUserName = apiUrlUserName
      , api_apiUrlPassword = apiUrlPassword
      , api_apiUrlInsecure = apiUrlInsecure
      } -> do
      httpRequest <- do
        httpRequest <- H.parseRequest apiUrl
        return $ if not (null apiUrlUserName) || not (null apiUrlPassword)
          then H.applyBasicAuth (fromString apiUrlUserName) (fromString apiUrlPassword) httpRequest
          else httpRequest
      initBlockChain BlockChainParams
        { bcp_httpManager = if apiUrlInsecure then httpInsecureManager else httpSecureManager
        , bcp_httpRequest = httpRequest
        , bcp_trace = trace
        , bcp_excludeUnaccountedActions = excludeUnaccountedActions
        , bcp_threadsCount = threadsCount
        }

    -- init output storages and begin block
    (outputStorages, _) <- do
      outputStorages <- initOutputStorages httpSecureManager output blockchainType heightFieldName hashFieldName flattenSuffixes
      let specifiedBeginBlock = if maybeBeginBlock >= 0 then maybeBeginBlock else defaultBeginBlock
      initContinuingOutputStorages outputStorages specifiedBeginBlock

    let subChainWriter = writeToOutputStorages outputStorages flattenPack
    let isBlockSaved = head $ map os_isBlockStored $ oss_storages outputStorages

    withSupervisor KillAll
      $ blockHashExporterSingle blockchains isBlockSaved subChainWriter

  -- ---------------------------------------------------------------------------
  OptionExportIotaCommand { } -> logStrLn "IOTA export not supported for realtime"
  OptionPrintSchemaCommand { } -> logStrLn "Print schema not supported for realtime"
