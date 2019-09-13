{-# LANGUAGE DeriveGeneric, DerivingStrategies, FlexibleInstances, GeneralizedNewtypeDeriving, LambdaCase, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Cosmos
  ( Cosmos(..)
  , CosmosBlock
  , CosmosTransaction(..)
  ) where

import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString.Short as BS
import Data.Int
import qualified Data.ProtocolBuffers.Internal as P
import Data.Proxy
import qualified Data.Serialize as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Numeric

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Util
import CoinMetrics.Tendermint
import CoinMetrics.Tendermint.Amino
import CoinMetrics.Util
import Hanalytics.Schema
import Hanalytics.Schema.Postgres

newtype Cosmos = Cosmos (Tendermint CosmosTransaction)

type CosmosBlock = TendermintBlock CosmosTransaction

newtype CosmosTransaction = CosmosTransaction J.Value deriving (SchemableField, J.FromJSON, J.ToJSON, A.ToAvro, A.HasAvroSchema, ToPostgresText)

instance TendermintTx CosmosTransaction where
  decodeTendermintTx t = do
    bytes <- either fail return $ BA.convertFromBase BA.Base64 $ T.encodeUtf8 t
    tx <- either fail return $ S.runGet readMsg bytes
    return $ CosmosTransaction $ J.toJSON (tx :: CosmosStdTx)

-- https://github.com/cosmos/cosmos-sdk/blob/master/docs/spec/auth/03_types.md#stdtx
data CosmosStdTx = CosmosStdTx
  { cst_msgs :: !(V.Vector CosmosMessage)
  , cst_fee :: !CosmosStdFee
  -- , cst_signatures :: !(V.Vector CosmosStdSignature)
  , cst_memo :: !(Maybe T.Text)
  }

instance ReadMsg CosmosStdTx where
  readMsg = withLen $ withPrefix 0xf0625dee $ withFields "CosmosStdTx" $ \h -> CosmosStdTx
    <$> readRepeated readSubStruct 1 h
    <*> readRequired readSubStruct 2 h
    -- <*> readRepeated readSubStruct 3 h
    <*> readOptional P.decodeWire 4 h


data CosmosStdSignature = CosmosStdSignature
  { css_pub_key :: !CosmosPubKey
  , css_signature :: {-# UNPACK #-} !HexString
  }

instance ReadMsg CosmosStdSignature where
  readMsg = withLen $ withFields "CosmosStdSignature" $ \h -> CosmosStdSignature
    <$> readRequired readSubStruct 1 h
    <*> readRequired P.decodeWire 2 h


newtype CosmosPubKey = CosmosPubKey
  { bpk_bytes :: HexString
  }

instance ReadMsg CosmosPubKey where
  readMsg = withPrefix 0xeb5ae987 $ CosmosPubKey . HexString . BS.toShort <$> (S.getBytes =<< P.getVarInt)


data CosmosStdFee = CosmosStdFee
  { csf_coins :: !(V.Vector CosmosCoin)
  , csf_gas :: {-# UNPACK #-} !Int64
  }

instance ReadMsg CosmosStdFee where
  readMsg = withFields "CosmosStdFee" $ \h -> CosmosStdFee
    <$> readRepeated readSubStruct 1 h
    <*> readRequired P.decodeWire 2 h


data CosmosMessage
  = CosmosMessage_Unknown
    { cm_tag :: !T.Text
    }
  | CosmosMessage_Send
    { cm_fromAddress :: {-# UNPACK #-} !HexString
    , cm_toAddress :: {-# UNPACK #-} !HexString
    , cm_coins :: !(V.Vector CosmosCoin)
    }
  | CosmosMessage_MultiSend
    { cm_inputs :: !(V.Vector CosmosInput)
    , cm_outputs :: !(V.Vector CosmosOutput)
    }
  | CosmosMessage_WithdrawDelegatorReward
    { cm_delegatorAddress :: {-# UNPACK #-} !HexString
    , cm_validatorAddress :: {-# UNPACK #-} !HexString
    }
  | CosmosMessage_WithdrawValidatorCommission
    { cm_validatorAddress :: {-# UNPACK #-} !HexString
    }
  | CosmosMessage_Delegate
    { cm_delegatorAddress :: {-# UNPACK #-} !HexString
    , cm_validatorAddress :: {-# UNPACK #-} !HexString
    , cm_amount :: !(V.Vector CosmosCoin)
    }
  | CosmosMessage_Undelegate
    { cm_delegatorAddress :: {-# UNPACK #-} !HexString
    , cm_validatorAddress :: {-# UNPACK #-} !HexString
    , cm_amount :: !(V.Vector CosmosCoin)
    }
  | CosmosMessage_CreateValidator
    { cm_description :: !CosmosDescription
    , cm_commission :: !CosmosCommissionMsg
    , cm_minSelfDelegation :: !(Maybe T.Text)
    , cm_delegatorAddress :: {-# UNPACK #-} !HexString
    , cm_validatorAddress :: {-# UNPACK #-} !HexString
    , cm_pubKey :: {-# UNPACK #-} !HexString
    , cm_value :: !CosmosCoin
    }
  | CosmosMessage_EditValidator
    { cm_description :: !CosmosDescription
    , cm_validatorAddress :: {-# UNPACK #-} !HexString
    , cm_commissionRate :: !(Maybe T.Text)
    , cm_minSelfDelegation :: !(Maybe T.Text)
    }
  | CosmosMessage_BeginRedelegate
    { cm_delegatorAddress :: {-# UNPACK #-} !HexString
    , cm_validatorSrcAddress :: {-# UNPACK #-} !HexString
    , cm_validatorDstAddress :: {-# UNPACK #-} !HexString
    , cm_amount :: !(V.Vector CosmosCoin)
    }
  | CosmosMessage_Vote
    { cm_proposalID :: {-# UNPACK #-} !Int64
    , cm_voter :: {-# UNPACK #-} !HexString
    , cm_option :: {-# UNPACK #-} !Int64
    }
  | CosmosMessage_SetWithdrawAddress
    { cm_delegatorAddress :: {-# UNPACK #-} !HexString
    , cm_withdrawADdress :: {-# UNPACK #-} !HexString
    }
  | CosmosMessage_Unjail
    { cm_validatorAddress :: {-# UNPACK #-} !HexString
    }
  | CosmosMessage_SubmitProposal
    { cm_title :: !T.Text
    , cm_desc :: !T.Text
    , cm_proposalRoute :: {-# UNPACK #-} !Int64
    , cm_proposer :: {-# UNPACK #-} !HexString
    , cm_initialDeposit :: !(V.Vector CosmosCoin)
    }
  | CosmosMessage_Deposit
    { cm_proposalID :: {-# UNPACK #-} !Int64
    , cm_depositor :: {-# UNPACK #-} !HexString
    , cm_amount :: !(V.Vector CosmosCoin)
    }


instance ReadMsg CosmosMessage where
  readMsg = do
    msgType <- S.getWord32be
    withFields ("CosmosMessage" <> showHex msgType "") $ \h -> case h `seq` msgType of
      0xa8a3619a -> CosmosMessage_Send
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRepeated readSubStruct 3 h
      0xc2689ad1 -> CosmosMessage_MultiSend
        <$> readRepeated readSubStruct 1 h
        <*> readRepeated readSubStruct 2 h
      0x8c4d710d-> CosmosMessage_WithdrawDelegatorReward
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
      0xcd3274b3 -> CosmosMessage_WithdrawValidatorCommission
        <$> readRequired P.decodeWire 1 h
      0x921d2e4e -> CosmosMessage_Delegate
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRepeated readSubStruct 3 h
      0x5c80810d -> CosmosMessage_Undelegate
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRepeated readSubStruct 3 h
      0xeb361d01 -> CosmosMessage_CreateValidator
        <$> readRequired readSubStruct 1 h
        <*> readRequired readSubStruct 2 h
        <*> readOptional P.decodeWire 3 h
        <*> readRequired P.decodeWire 4 h
        <*> readRequired P.decodeWire 5 h
        <*> readRequired P.decodeWire 6 h
        <*> readRequired readSubStruct 7 h
      0xc2e8bccd -> CosmosMessage_EditValidator
        <$> readRequired readSubStruct 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readOptional P.decodeWire 3 h
        <*> readOptional P.decodeWire 4 h
      0x9c959346 -> CosmosMessage_BeginRedelegate
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRequired P.decodeWire 3 h
        <*> readRepeated readSubStruct 4 h
      0xa1cadd36 -> CosmosMessage_Vote
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRequired P.decodeWire 3 h
      0x536070b8 -> CosmosMessage_SetWithdrawAddress
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
      0x543aec70 -> CosmosMessage_Unjail
        <$> readRequired P.decodeWire 1 h
      0xb42d614e -> CosmosMessage_SubmitProposal
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRequired P.decodeWire 3 h
        <*> readRequired P.decodeWire 4 h
        <*> readRepeated readSubStruct 5 h
      0xa18a56e5 -> CosmosMessage_Deposit
        <$> readRequired P.decodeWire 1 h
        <*> readRequired P.decodeWire 2 h
        <*> readRepeated readSubStruct 3 h
      _ -> return $ CosmosMessage_Unknown $ T.pack $ showHex msgType ""


data CosmosCoin = CosmosCoin
  { bt_denom :: !(Maybe T.Text)
  , bt_amount :: !Integer
  }

instance ReadMsg CosmosCoin where
  readMsg = withFields "CosmosCoin" $ \h -> CosmosCoin
    <$> readOptional P.decodeWire 1 h
    <*> (read <$> readRequired P.decodeWire 2 h)


data CosmosInput = CosmosInput
  { ci_address :: {-# UNPACK #-} !HexString
  , ci_coins :: !(V.Vector CosmosCoin)
  }

instance ReadMsg CosmosInput where
  readMsg = withFields "CosmosInput" $ \h -> CosmosInput
    <$> readRequired P.decodeWire 1 h
    <*> readRepeated readSubStruct 2 h

data CosmosOutput = CosmosOutput
  { co_address :: {-# UNPACK #-} !HexString
  , co_coins :: !(V.Vector CosmosCoin)
  }

instance ReadMsg CosmosOutput where
  readMsg = withFields "CosmosOutput" $ \h -> CosmosOutput
    <$> readRequired P.decodeWire 1 h
    <*> readRepeated readSubStruct 2 h


data CosmosDescription = CosmosDescription
  { cd_moniker :: !T.Text
  , cd_identity :: !(Maybe T.Text)
  , cd_website :: !(Maybe T.Text)
  , cd_details :: !(Maybe T.Text)
  }

instance ReadMsg CosmosDescription where
  readMsg = withFields "CosmosDescription" $ \h -> CosmosDescription
    <$> readRequired P.decodeWire 1 h
    <*> readOptional P.decodeWire 2 h
    <*> readOptional P.decodeWire 3 h
    <*> readOptional P.decodeWire 4 h


data CosmosCommissionMsg = CosmosCommissionMsg
  { ccm_rate :: !T.Text
  , ccm_maxRate :: !T.Text
  , ccm_maxChangeRate :: !T.Text
  }

instance ReadMsg CosmosCommissionMsg where
  readMsg = withFields "CosmosCommissionMsg" $ \h -> CosmosCommissionMsg
    <$> readRequired P.decodeWire 1 h
    <*> readRequired P.decodeWire 2 h
    <*> readRequired P.decodeWire 3 h


genJsonInstances [''CosmosStdTx, ''CosmosStdSignature, ''CosmosPubKey, ''CosmosStdFee, ''CosmosMessage, ''CosmosCoin, ''CosmosInput, ''CosmosOutput, ''CosmosDescription, ''CosmosCommissionMsg]

instance BlockChain Cosmos where
  type Block Cosmos = CosmosBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = fmap Cosmos . bci_init (getBlockChainInfo undefined)
    , bci_defaultApiUrls = ["http://127.0.0.1:26657/"]
    , bci_defaultBeginBlock = 1
    , bci_defaultEndBlock = 0
    , bci_heightFieldName = "height"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy CosmosBlock))
      [
      ]
      "CREATE TABLE \"cosmos\" OF \"CosmosBlock\" (PRIMARY KEY (\"height\"));"
    }

  getCurrentBlockHeight (Cosmos tendermint) = getCurrentBlockHeight tendermint

  getBlockByHeight (Cosmos tendermint) = getBlockByHeight tendermint
