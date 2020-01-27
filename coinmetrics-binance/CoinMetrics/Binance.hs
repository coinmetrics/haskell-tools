{-# LANGUAGE DeriveGeneric, DerivingStrategies, FlexibleInstances, GeneralizedNewtypeDeriving, LambdaCase, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Binance
  ( Binance(..)
  , BinanceBlock
  , BinanceTransaction(..)
  ) where

import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import Data.Int
import qualified Data.ProtocolBuffers.Internal as P
import Data.Proxy
import qualified Data.Serialize as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Data.Word

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Util
import CoinMetrics.Tendermint
import CoinMetrics.Tendermint.Amino
import CoinMetrics.Util
import Hanalytics.Schema
import Hanalytics.Schema.Postgres

newtype Binance = Binance (Tendermint BinanceTransaction)

type BinanceBlock = TendermintBlock BinanceTransaction

newtype BinanceTransaction = BinanceTransaction J.Value deriving (SchemableField, J.FromJSON, J.ToJSON, A.ToAvro, A.HasAvroSchema, ToPostgresText)

-- https://docs.binance.org/encoding.html#binance-chain-transaction-encoding

instance TendermintTx BinanceTransaction where
  decodeTendermintTx t = do
    bytes <- either fail return $ BA.convertFromBase BA.Base64 $ T.encodeUtf8 t
    tx <- either fail return $ S.runGet readMsg bytes
    return $ BinanceTransaction $ J.toJSON (tx :: BinanceStdTx)

data BinanceStdTx = BinanceStdTx
  { bst_msgs :: !(V.Vector BinanceMessage)
  -- , bst_signatures :: !(V.Vector BinanceStdSignature)
  , bst_memo :: !(Maybe T.Text)
  , bst_source :: !(Maybe Int64)
  , bst_data :: !(Maybe HexString)
  }

instance ReadMsg BinanceStdTx where
  readMsg = withLen $ withPrefix 0xF0625DEE $ withFields "BinanceStdTx" $ \h -> BinanceStdTx
    <$> readRepeated readSubStruct 1 h
    -- <*> readRepeated readSubStruct 2 h
    <*> readOptional P.decodeWire 3 h
    <*> readOptional P.decodeWire 4 h
    <*> readOptional P.decodeWire 5 h

data BinanceStdSignature = BinanceStdSignature
  { bss_pub_key :: !BinancePubKey
  , bss_signature :: {-# UNPACK #-} !HexString
  , bss_account_number :: {-# UNPACK #-} !Int64
  , bss_sequence :: {-# UNPACK #-} !Int64
  }

instance ReadMsg BinanceStdSignature where
  readMsg = withLen $ withFields "BinanceStdSignature" $ \h -> BinanceStdSignature
      <$> readRequired readSubStruct 1 h
      <*> readRequired P.decodeWire 2 h
      <*> readRequired P.decodeWire 3 h
      <*> readRequired P.decodeWire 4 h

newtype BinancePubKey = BinancePubKey
  { bpk_bytes :: HexString
  }

instance ReadMsg BinancePubKey where
  readMsg = withPrefix 0xEB5AE987 $ BinancePubKey . HexString . BS.toShort <$> (S.getBytes =<< P.getVarInt)

-- https://github.com/binance-chain/go-sdk/blob/master/types/msg

data BinanceMessage
  = BinanceMessage_Unknown
    { bm_tag :: {-# UNPACK #-} !Word32
    }
  | BinanceMessage_Send
    { bm_inputs :: !(V.Vector BinanceInput)
    , bm_outputs :: !(V.Vector BinanceOutput)
    }
  | BinanceMessage_NewOrder
    { bm_sender :: {-# UNPACK #-} !HexString
    , bm_id :: !T.Text
    , bm_symbol :: !T.Text
    , bm_ordertype :: {-# UNPACK #-} !Int64
    , bm_side :: {-# UNPACK #-} !Int64
    , bm_price :: {-# UNPACK #-} !Int64
    , bm_quantity :: {-# UNPACK #-} !Int64
    , bm_timeinforce :: {-# UNPACK #-} !Int64
    }
  | BinanceMessage_CancelOrder
    { bm_sender :: {-# UNPACK #-} !HexString
    , bm_symbol :: !T.Text
    , bm_refid :: !T.Text
    }
  | BinanceMessage_TokenFreeze
    { bm_from :: {-# UNPACK #-} !HexString
    , bm_symbol :: !T.Text
    , bm_amount :: {-# UNPACK #-} !Int64
    }
  | BinanceMessage_TokenUnfreeze
    { bm_from :: {-# UNPACK #-} !HexString
    , bm_symbol :: !T.Text
    , bm_amount :: {-# UNPACK #-} !Int64
    }
  | BinanceMessage_Vote
    { bm_proposal_id :: {-# UNPACK #-} !Int64
    , bm_voter :: {-# UNPACK #-} !HexString
    , bm_option :: {-# UNPACK #-} !Int64
    }
  | BinanceMessage_IssueTokenValue
    { bm_from :: {-# UNPACK #-} !HexString
    , bm_name :: !T.Text
    , bm_symbol :: !T.Text
    , bm_total_supply :: {-# UNPACK #-} !Int64
    , bm_mintable :: !(Maybe Bool)
    }
  | BinanceMessage_Mint
    { bm_from :: {-# UNPACK #-} !HexString
    , bm_symbol :: !T.Text
    , bm_amount :: {-# UNPACK #-} !Int64
    }
  | BinanceMessage_TokenBurn
    { bm_from :: {-# UNPACK #-} !HexString
    , bm_symbol :: !T.Text
    , bm_amount :: {-# UNPACK #-} !Int64
    }
  | BinanceMessage_DexList
    { bm_from :: {-# UNPACK #-} !HexString
    , bm_proposal_id :: {-# UNPACK #-} !Int64
    , bm_base_asset_symbol :: !T.Text
    , bm_quote_asset_symbol :: !T.Text
    , bm_init_price :: {-# UNPACK #-} !Int64
    }
  | BinanceMessage_SubmitProposal
    { bm_title :: !T.Text
    , bm_description :: !J.Value
    , bm_proposal_type :: !Int64
    , bm_proposer :: {-# UNPACK #-} !HexString
    , bm_initial_deposit :: !(V.Vector BinanceToken)
    , bm_voting_period :: {-# UNPACK #-} !Int64
    }
  | BinanceMessage_Deposit
    { bm_proposal_id :: {-# UNPACK #-} !Int64
    , bm_depositer :: {-# UNPACK #-} !HexString
    }

instance ReadMsg BinanceMessage where
  readMsg = do
    msgType <- S.getWord32be
    withFields ("BinanceMessage" <> show msgType) $ \h -> case msgType of
      0x2A2C87FA -> BinanceMessage_Send
        <$> readRepeated readSubStruct 1 h
        <*> readRepeated readSubStruct 2 h
      0xCE6DC043 -> BinanceMessage_NewOrder
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRequired P.decodeWire 3 h
        <*> readRequired P.decodeWire 4 h
        <*> readRequired P.decodeWire 5 h
        <*> readRequired P.decodeWire 6 h
        <*> readRequired P.decodeWire 7 h
        <*> readRequired P.decodeWire 8 h
      0x166E681B -> BinanceMessage_CancelOrder
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRequired P.decodeWire 3 h
      0xE774B32D -> BinanceMessage_TokenFreeze
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRequired P.decodeWire 3 h
      0x6515FF0D -> BinanceMessage_TokenUnfreeze
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRequired P.decodeWire 3 h
      0xA1CADD36 -> BinanceMessage_Vote
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRequired P.decodeWire 3 h
      0x17EFAB80 -> BinanceMessage_IssueTokenValue
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRequired P.decodeWire 3 h
        <*> readRequired P.decodeWire 4 h
        <*> readOptional P.decodeWire 5 h
      0x467E0829 -> BinanceMessage_Mint
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRequired P.decodeWire 3 h
      0x7ED2D2A0 -> BinanceMessage_TokenBurn
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRequired P.decodeWire 3 h
      0xB41DE13F -> BinanceMessage_DexList
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRequired P.decodeWire 3 h
        <*> readRequired P.decodeWire 4 h
        <*> readRequired P.decodeWire 5 h
      0xB42D614E -> BinanceMessage_SubmitProposal
        <$> readRequired P.decodeWire 1 h
        <*> (either fail return . J.eitherDecode . BL.fromStrict . T.encodeUtf8 =<< readRequired P.decodeWire 2 h)
        <*> readRequired P.decodeWire 3 h
        <*> readRequired P.decodeWire 4 h
        <*> readRepeated readSubStruct 5 h
        <*> readRequired P.decodeWire 6 h
      0xA18A56E5 -> BinanceMessage_Deposit
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
      _ -> return $ BinanceMessage_Unknown msgType

data BinanceInput = BinanceInput
  { bi_address :: {-# UNPACK #-} !HexString
  , bi_coins :: !(V.Vector BinanceToken)
  }

instance ReadMsg BinanceInput where
  readMsg = withFields "BinanceInput" $ \h -> BinanceInput
    <$> readRequired P.decodeWire 1 h
    <*> readRepeated readSubStruct 2 h

data BinanceOutput = BinanceOutput
  { bo_address :: {-# UNPACK #-} !HexString
  , bo_coins :: !(V.Vector BinanceToken)
  }

instance ReadMsg BinanceOutput where
  readMsg = withFields "BinanceOutput" $ \h -> BinanceOutput
    <$> readRequired P.decodeWire 1 h
    <*> readRepeated readSubStruct 2 h

data BinanceToken = BinanceToken
  { bt_denom :: !T.Text
  , bt_amount :: {-# UNPACK #-} !Int64
  }

instance ReadMsg BinanceToken where
  readMsg = withFields "BinanceToken" $ \h -> BinanceToken
    <$> readRequired P.decodeWire 1 h
    <*> readRequired P.decodeWire 2 h

genJsonInstances [''BinanceStdTx, ''BinanceStdSignature, ''BinancePubKey, ''BinanceMessage, ''BinanceInput, ''BinanceOutput, ''BinanceToken]

instance BlockChain Binance where
  type Block Binance = BinanceBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = fmap Binance . bci_init (getBlockChainInfo undefined)
    , bci_defaultApiUrls = ["http://127.0.0.1:27147/"]
    , bci_defaultBeginBlock = 1
    , bci_defaultEndBlock = 0
    , bci_heightFieldName = "height"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy BinanceBlock))
      [
      ]
      "CREATE TABLE \"binance\" OF \"BinanceBlock\" (PRIMARY KEY (\"height\"));"
    }

  -- that returns Tendermint SDK's version, not Binance's
  -- getBlockChainNodeInfo (Binance tendermint) = getBlockChainNodeInfo tendermint

  getCurrentBlockHeight (Binance tendermint) = getCurrentBlockHeight tendermint

  getBlockByHeight (Binance tendermint) = getBlockByHeight tendermint
