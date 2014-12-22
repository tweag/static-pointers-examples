{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RecursiveDo #-}

module Control.Distributed.Process where

import qualified Network.Transport as NT

import Control.Applicative
import Control.Exception (finally, throwIO)
import Control.Monad.IO.Class
import Control.Monad.Reader
import Control.Concurrent
import Data.Binary (Binary, decode, encode)
import Data.ByteString.Lazy as BSL (ByteString, fromChunks)
import Data.Int (Int32)
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Typeable (Typeable)
import Data.Typeable.Internal
import GHC.Generics (Generic)
import GHC.Fingerprint (Fingerprint)
import System.IO (fixIO)

data Message = Message
  { messageFingerPrint :: !Fingerprint
  , messageEncoding    :: !BSL.ByteString
  } deriving (Generic, Typeable)

instance Binary Message

newtype NodeId = NodeId { nodeAddress :: NT.EndPointAddress }
  deriving (Eq, Ord, Generic, Typeable)

instance Binary NodeId

data ProcessId = ProcessId
  { processNodeId  :: !NodeId
  , processLocalId :: !Int32
  } deriving (Eq, Ord, Generic, Typeable)

instance Binary ProcessId

data LocalNode = LocalNode
  { localNodeId   :: !NodeId
  , localEndPoint :: !NT.EndPoint
  , localState    :: !(MVar LocalNodeState)
  }

data LocalNodeState = LocalNodeState
  { localProcesses  :: !(Map ProcessId LocalProcess)
  , localPidCounter :: !Int32
  }

data LocalProcess = LocalProcess
  { processMailbox     :: !(Chan Message)
  , processId          :: !ProcessId
  , processThread      :: !ThreadId
  , processNode        :: !LocalNode
  , processConnections :: !(Map ProcessId NT.Connection)
  }

newtype Process a = Process { unProcess :: ReaderT LocalProcess IO a }
  deriving (Applicative, Functor, Monad, MonadIO, Typeable)

class (Binary a, Typeable a) => Serializable a
instance (Binary a, Typeable a) => Serializable a

expect = undefined

send :: Serializable a => ProcessId -> a -> Process ()
send them x = do
    us <- processId <$> Process $ ask
    conn <- connectionBetween us them
    NT.send conn [encode $ tyConHash $ typeRepTyCon $ typeRep x, encode x]
  where
    connectionBetween = do
      lproc <- Process $ ask
      connections <- processConnections lproc
      Map.lookup pid connections >>= \case
        Nothing -> NT.connect (localEndPoint $ processNode lproc)
                              (nodeAddress $ localNodeId $ processNode lproc)
                              NT.ReliableOrdered
                              NT.defaultConnectHints

handleIncomingMessages :: LocalNode -> IO ()
handleIncomingMessages LocalNode{..} =
    go (Map.empty :: Map NT.ConnectionId (Maybe (Chan Message)))
  where
    go conns = do
      event <- NT.receive localEndPoint
      case event of
        NT.ConnectionOpened cid _ _ -> do
            go (Map.insert cid Nothing conns)
        NT.Received cid payload
          | Nothing <- conns Map.! cid -> do
              let pid = decode (BSL.fromChunks payload)
              chan <- undefined (pid :: ProcessId)
              go (Map.insert cid chan conns)
          | Just chan <- conns Map.! cid -> do
              writeChan chan (decode (BSL.fromChunks payload))
        NT.ConnectionClosed cid -> do
            go (Map.delete cid conns)
        _ -> fail "Unexpected event."

data ControlMessage = ControlMessage
  { controlMessageSender :: !ProcessId
  , controlMessageSignal :: !ProcessSignal
  }

data ProcessSignal
  = Spawn !(StaticPtr (Process ()))
  | Kill ProcessId

nodeController :: Process ()
nodeController = forever $ do
    ctrlMsg <- expect
    case ctrlMsg of
      Spawn clos -> forkProcess (deRefStaticPtr clos)
      Kill pid -> do
        LocalProcess{processNode} <- Process $ ask
        withMVar (localState processNode) $ \LocalNodeState{..} -> do
          forM_ (Map.lookup pid localProcesses) killThread

nodeControllerPid :: NodeId -> ProcessId
nodeControllerPid nid = ProcessId
    { processNodeId = nid
    , processLocalId = 0
    }

createBareLocalNode :: NT.EndPoint -> IO LocalNode
createBareLocalNode endPoint = do
    state <- newMVar LocalNodeState
      { localProcesses = Map.empty
      , localPidCounter = 1
      }
    let nid = NodeId $ NT.address endPoint
        lnode = LocalNode
          { localNodeId = nid
          , localEndPoint = endPoint
          , localState = state
          }
    pid <- forkProcess $ nodeController `finally` NT.closeEndPoint endPoint
    _ <- forkIO $ handleIncomingMessages lnode
           `finally` send (nodeControllerPid nid) (Kill pid)
    return lnode

forkProcess :: LocalNode -> Process () -> IO ProcessId
forkProcess lnode action = do
    liftIO $ modifyMVarMasked (localState lnode) startProcess
  where
    startProcess :: LocalNodeState -> IO (LocalNodeState, ProcessId)
    startProcess LocalNodeState{..} = do
        let pid  = ProcessId { processNodeId = localNodeId lnode
                             , processLocalId = localPidCounter
                             }
        chan <- newChan
        (_, lproc) <- fixIO $ \ ~(tid, _) -> do
          let lproc = LocalProcess
                { processMailbox = chan
                , processId = pid
                , processThread = tid
                , ..
                }
          tid' <- forkIO $ do
            runReaderT (unProcess action) lproc `finally` do
              -- Cleanup when the process dies.
              modifyMVar_ (localState lnode) $ \st ->
                st{ localProcesses = Map.delete pid (localProcesses st) }
          return (tid', lproc)
        return ( LocalNodeState
                   { localProcesses = Map.insert pid lproc localProcesses
                   , localPidCounter = localPidCounter + 1
                   }
               , pid
               )

runProcess :: NT.Transport -> Process () -> IO ()
runProcess transport action = do
    lnode <- either throwIO createBareLocalNode <$> NT.newEndPoint
    return undefined
