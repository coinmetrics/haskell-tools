{-# LANGUAGE CPP, OverloadedStrings #-}

module CoinMetrics.BlockChain.All
  ( getSomeBlockChainInfo
  , allBlockChainTypes
  , blockchainTypesStr
  ) where

import qualified Data.HashMap.Strict as HM
import Data.Proxy
import qualified Data.Text as T

import CoinMetrics.BlockChain

#if defined(CM_SUPPORT_BINANCE)
import CoinMetrics.Binance
#endif
#if defined(CM_SUPPORT_BITCOIN)
import CoinMetrics.Bitcoin
#endif
#if defined(CM_SUPPORT_CARDANO)
import CoinMetrics.Cardano
#endif
#if defined(CM_SUPPORT_COSMOS)
import CoinMetrics.Cosmos
#endif
#if defined(CM_SUPPORT_EOS)
import CoinMetrics.EOS
#endif
#if defined(CM_SUPPORT_ETHEREUM)
import CoinMetrics.Ethereum
#endif
#if defined(CM_SUPPORT_GRIN)
import CoinMetrics.Grin
#endif
#if defined(CM_SUPPORT_MONERO)
import CoinMetrics.Monero
#endif
#if defined(CM_SUPPORT_NEM)
import CoinMetrics.Nem
#endif
#if defined(CM_SUPPORT_NEO)
import CoinMetrics.Neo
#endif
#if defined(CM_SUPPORT_RIPPLE)
import CoinMetrics.Ripple
#endif
#if defined(CM_SUPPORT_STELLAR)
import CoinMetrics.Stellar
#endif
#if defined(CM_SUPPORT_TENDERMINT)
import CoinMetrics.Tendermint
#endif
#if defined(CM_SUPPORT_TEZOS)
import CoinMetrics.Tezos
#endif
#if defined(CM_SUPPORT_TRON)
import CoinMetrics.Tron
#endif
#if defined(CM_SUPPORT_WAVES)
import CoinMetrics.Waves
#endif

allBlockChainInfos :: HM.HashMap T.Text SomeBlockChainInfo
allBlockChainInfos = HM.fromList infos
  where
    infos =
#if defined(CM_SUPPORT_BINANCE)
      ("binance",  SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Binance)) :
#endif

#if defined(CM_SUPPORT_BITCOIN)
      ("bitcoin",  SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Bitcoin)) :
#endif

#if defined(CM_SUPPORT_CARDANO)
      ("cardano",  SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Cardano)) :
#endif

#if defined(CM_SUPPORT_COSMOS)
      ("cosmos",  SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Cosmos)) :
#endif

#if defined(CM_SUPPORT_EOS)
      ("eos",      SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Eos)) :
#endif

#if defined(CM_SUPPORT_ETHEREUM)
      ("ethereum", SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Ethereum)) :
#endif

#if defined(CM_SUPPORT_GRIN)
      ("grin",  SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Grin)) :
#endif

#if defined(CM_SUPPORT_MONERO)
      ("monero",   SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Monero)) :
#endif

#if defined(CM_SUPPORT_NEM)
      ("nem",      SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Nem)) :
#endif

#if defined(CM_SUPPORT_NEO)
      ("neo",      SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Neo)) :
#endif

#if defined(CM_SUPPORT_RIPPLE)
      ("ripple",   SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Ripple)) :
#endif

#if defined(CM_SUPPORT_STELLAR)
      ("stellar",  SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Stellar)) :
#endif

#if defined(CM_SUPPORT_TENDERMINT)
      ("tendermint",  SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy (Tendermint T.Text))) :
#endif

#if defined(CM_SUPPORT_TEZOS)
      ("tezos",  SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Tezos)) :
#endif

#if defined(CM_SUPPORT_TRON)
      ("tron",  SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Tron)) :
#endif

#if defined(CM_SUPPORT_WAVES)
      ("waves",    SomeBlockChainInfo $ getBlockChainInfo (Proxy :: Proxy Waves)) :
#endif
  
      []

-- | Get blockchain info by name.
getSomeBlockChainInfo :: T.Text -> Maybe SomeBlockChainInfo
getSomeBlockChainInfo = flip HM.lookup allBlockChainInfos

-- | Get supported blockchain types.
allBlockChainTypes :: [T.Text]
allBlockChainTypes = HM.keys allBlockChainInfos

blockchainTypesStr :: String
blockchainTypesStr = T.unpack $ T.intercalate ", " allBlockChainTypes
