package DHashtable;

/****** SALSA LANGUAGE IMPORTS ******/
import salsa_lite.common.DeepCopy;
import salsa_lite.runtime.MobileActorRegistry;
import salsa_lite.runtime.wwc.NameServer;
import salsa_lite.runtime.Hashing;
import salsa_lite.runtime.Acknowledgement;
import salsa_lite.runtime.SynchronousMailboxStage;
import salsa_lite.runtime.Actor;
import salsa_lite.runtime.Message;
import salsa_lite.runtime.RemoteActor;
import salsa_lite.runtime.MobileActor;
import salsa_lite.runtime.StageService;
import salsa_lite.runtime.TransportService;
import salsa_lite.runtime.language.Director;
import salsa_lite.runtime.language.JoinDirector;
import salsa_lite.runtime.language.MessageDirector;
import salsa_lite.runtime.language.ContinuationDirector;
import salsa_lite.runtime.language.TokenDirector;

import salsa_lite.runtime.language.exceptions.RemoteMessageException;
import salsa_lite.runtime.language.exceptions.TokenPassException;
import salsa_lite.runtime.language.exceptions.MessageHandlerNotFoundException;
import salsa_lite.runtime.language.exceptions.ConstructorNotFoundException;

import salsa_lite.runtime.wwc.OutgoingTheaterConnection;

/****** END SALSA LANGUAGE IMPORTS ******/

import java.io.PrintStream;
import javasrc.ChordKey;
import salsa_lite.runtime.io.StandardOutput;
import java.util.Hashtable;

public class ChordNode extends MobileActor implements java.io.Serializable {

    public Object writeReplace() throws java.io.ObjectStreamException {
        return new SerializedChordNode( this.getName(), this.getNameServer(), this.getLastKnownHost(), this.getLastKnownPort());
    }

    public static class SerializedChordNode implements java.io.Serializable {
        String name;
        String lastKnownHost;
        int lastKnownPort;

        NameServer nameserver;

        SerializedChordNode(String name, NameServer nameserver, String lastKnownHost, int lastKnownPort) { this.name = name; this.nameserver = nameserver; this.lastKnownHost = lastKnownHost; this.lastKnownPort = lastKnownPort; }

        public Object readResolve() throws java.io.ObjectStreamException {
            int hashCode = Hashing.getHashCodeFor(name, nameserver.getName(), nameserver.getHost(), nameserver.getPort());

                synchronized (MobileActorRegistry.getStateLock(hashCode)) {
                    Actor entry = MobileActorRegistry.getStateEntry(hashCode);
                    if (entry == null) {
                        MobileActorRegistry.addStateEntry(hashCode, TransportService.getSocket(lastKnownHost, lastKnownPort));
                    }
                }

            synchronized (MobileActorRegistry.getReferenceLock(hashCode)) {
                ChordNode actor = (ChordNode)MobileActorRegistry.getReferenceEntry(hashCode);
                if (actor == null) {
                    ChordNode remoteReference = new ChordNode(name, nameserver, lastKnownHost, lastKnownPort);
                    MobileActorRegistry.addReferenceEntry(hashCode, remoteReference);
                    return remoteReference;
                } else {
                    return actor;
                }
            }
        }
    }

    public Object invokeMessage(int messageId, Object[] arguments) throws RemoteMessageException, TokenPassException, MessageHandlerNotFoundException {
        Object entry;
        int hashCode = hashCode();
        synchronized (MobileActorRegistry.getStateLock(hashCode)) {
            entry = MobileActorRegistry.getStateEntry(hashCode);
        }
        if (entry instanceof State) {
            return ((State)entry).invokeMessage(messageId, arguments);
        } else {
            StageService.sendMessage(new Message(Message.SIMPLE_MESSAGE, ((OutgoingTheaterConnection)entry), 2 /*send*/, new Object[]{this.getStage().message}));
            throw new RemoteMessageException();
        }
    }

    public void invokeConstructor(int messageId, Object[] arguments) throws RemoteMessageException, ConstructorNotFoundException {
        Object entry;
        int hashCode = hashCode();
        synchronized (MobileActorRegistry.getStateLock(hashCode)) {
            entry = MobileActorRegistry.getStateEntry(hashCode);
        }
        if (entry instanceof State) {
            ((State)entry).invokeConstructor(messageId, arguments);
        } else {
            StageService.sendMessage(new Message(Message.SIMPLE_MESSAGE, ((OutgoingTheaterConnection)entry), 2 /*send*/, new Object[]{this.getStage().message}));
            throw new RemoteMessageException();
        }
    }


    public ChordNode(String name, NameServer nameserver) { super(name, nameserver); }
    public ChordNode(String name, NameServer nameserver, int stage_id) { super(name, nameserver, stage_id); }
    public ChordNode(String name, NameServer nameserver, String lastKnownHost, int lastKnownPort) { super(name, nameserver, lastKnownHost, lastKnownPort); }

    public ChordNode(String name, NameServer nameserver, String lastKnownHost, int lastKnownPort, int stage_id) { super(name, nameserver, lastKnownHost, lastKnownPort, stage_id); }

    public static ChordNode construct(int constructor_id, Object[] arguments, String name, NameServer nameserver) {
        ChordNode actor = new ChordNode(name, nameserver);
        State state = new State(name, nameserver, actor.getStageId());

        StageService.sendMessage(new Message(Message.SIMPLE_MESSAGE, nameserver, 4 /*put*/, new Object[]{actor})); //register the actor with the name server. 

        StageService.sendMessage(new Message(Message.CONSTRUCT_MESSAGE, actor, constructor_id, arguments));
        return actor;
    }

    public static ChordNode construct(int constructor_id, Object[] arguments, String name, NameServer nameserver, int target_stage_id) {
        ChordNode actor = new ChordNode(name, nameserver, target_stage_id);
        State state = new State(name, nameserver, target_stage_id);

        StageService.sendMessage(new Message(Message.SIMPLE_MESSAGE, nameserver, 4 /*put*/, new Object[]{actor})); //register the actor with the name server. 

        actor.getStage().putMessageInMailbox(new Message(Message.CONSTRUCT_MESSAGE, actor, constructor_id, arguments));
        return actor;
    }


    public static TokenDirector construct(int constructor_id, Object[] arguments, int[] token_positions, String name, NameServer nameserver) {
        ChordNode actor = new ChordNode(name, nameserver);
        State state = new State(name, nameserver, actor.getStageId());

        StageService.sendMessage(new Message(Message.SIMPLE_MESSAGE, nameserver, 4 /*put*/, new Object[]{actor})); //register the actor with the name server. 

        TokenDirector output_continuation = TokenDirector.construct(0 /*construct()*/, null);
        Message input_message = new Message(Message.CONSTRUCT_CONTINUATION_MESSAGE, actor, constructor_id, arguments, output_continuation);
        MessageDirector md = MessageDirector.construct(3, new Object[]{input_message, arguments, token_positions});
        return output_continuation;
    }

    public static TokenDirector construct(int constructor_id, Object[] arguments, int[] token_positions, String name, NameServer nameserver, int target_stage_id) {
        ChordNode actor = new ChordNode(name, nameserver, target_stage_id);
        State state = new State(name, nameserver, target_stage_id);

        StageService.sendMessage(new Message(Message.SIMPLE_MESSAGE, nameserver, 4 /*put*/, new Object[]{actor})); //register the actor with the name server. 

        TokenDirector output_continuation = TokenDirector.construct(0 /*construct()*/, null, target_stage_id);
        Message input_message = new Message(Message.CONSTRUCT_CONTINUATION_MESSAGE, actor, constructor_id, arguments, output_continuation);
        MessageDirector md = MessageDirector.construct(3, new Object[]{input_message, arguments, token_positions}, target_stage_id);
        return output_continuation;
    }

    public static TokenDirector construct(int constructor_id, Object[] arguments, String name, NameServer nameserver, String host, int port) {
        ChordNode actor = new ChordNode(name, nameserver, host, port);
        State state = new State(name, nameserver, host, port, actor.getStageId());

        if (! (host.equals(TransportService.getHost()) && port == TransportService.getPort()) ) {
            synchronized (MobileActorRegistry.getStateLock(actor.hashCode())) {
                MobileActorRegistry.updateStateEntry(actor.hashCode(), TransportService.getSocket(host, port));
            }
            TransportService.migrateActor(host, port, state);
        }

        StageService.sendMessage(new Message(Message.SIMPLE_MESSAGE, nameserver, 4 /*put*/, new Object[]{actor})); //register the actor with the name server. 

        TokenDirector output_continuation = TokenDirector.construct(0 /*construct()*/, null);
        StageService.sendMessage(new Message(Message.CONSTRUCT_CONTINUATION_MESSAGE, actor, constructor_id, arguments, output_continuation));
        return output_continuation;
    }

    public static TokenDirector construct(int constructor_id, Object[] arguments, String name, NameServer nameserver, String host, int port, int target_stage_id) {
        ChordNode actor = new ChordNode(name, nameserver, host, port, target_stage_id);
        State state = new State(name, nameserver, host, port, target_stage_id);

        if (! (host.equals(TransportService.getHost()) && port == TransportService.getPort()) ) {
            synchronized (MobileActorRegistry.getStateLock(actor.hashCode())) {
                MobileActorRegistry.updateStateEntry(actor.hashCode(), TransportService.getSocket(host, port));
            }
            TransportService.migrateActor(host, port, state);
        }

        StageService.sendMessage(new Message(Message.SIMPLE_MESSAGE, nameserver, 4 /*put*/, new Object[]{actor})); //register the actor with the name server. 

        TokenDirector output_continuation = TokenDirector.construct(0 /*construct()*/, null);
        StageService.sendMessage(new Message(Message.CONSTRUCT_CONTINUATION_MESSAGE, actor, constructor_id, arguments, output_continuation));
        return output_continuation;
    }

    public static TokenDirector construct(int constructor_id, Object[] arguments, int[] token_positions, String name, NameServer nameserver, String host, int port) {
        ChordNode actor = new ChordNode(name, nameserver, host, port);
        State state = new State(name, nameserver, host, port, actor.getStageId());

        if (! (host.equals(TransportService.getHost()) && port == TransportService.getPort()) ) {
            synchronized (MobileActorRegistry.getStateLock(actor.hashCode())) {
                MobileActorRegistry.updateStateEntry(actor.hashCode(), TransportService.getSocket(host, port));
            }
            TransportService.migrateActor(host, port, state);
        }

        StageService.sendMessage(new Message(Message.SIMPLE_MESSAGE, nameserver, 4 /*put*/, new Object[]{actor})); //register the actor with the name server. 

        TokenDirector output_continuation = TokenDirector.construct(0 /*construct()*/, null);
        Message input_message = new Message(Message.CONSTRUCT_CONTINUATION_MESSAGE, actor, constructor_id, arguments, output_continuation);
        MessageDirector md = MessageDirector.construct(3, new Object[]{input_message, arguments, token_positions});
        return output_continuation;
    }

    public static TokenDirector construct(int constructor_id, Object[] arguments, int[] token_positions, String name, NameServer nameserver, String host, int port, int target_stage_id) {
        ChordNode actor = new ChordNode(name, nameserver, host, port, target_stage_id);
        State state = new State(name, nameserver, host, port, target_stage_id);

        if (! (host.equals(TransportService.getHost()) && port == TransportService.getPort()) ) {
            synchronized (MobileActorRegistry.getStateLock(actor.hashCode())) {
                MobileActorRegistry.updateStateEntry(actor.hashCode(), TransportService.getSocket(host, port));
            }
            TransportService.migrateActor(host, port, state);
        }

        StageService.sendMessage(new Message(Message.SIMPLE_MESSAGE, nameserver, 4 /*put*/, new Object[]{actor})); //register the actor with the name server. 

        TokenDirector output_continuation = TokenDirector.construct(0 /*construct()*/, null, target_stage_id);
        Message input_message = new Message(Message.CONSTRUCT_CONTINUATION_MESSAGE, actor, constructor_id, arguments, output_continuation);
        MessageDirector md = MessageDirector.construct(3, new Object[]{input_message, arguments, token_positions}, target_stage_id);
        return output_continuation;
    }


    public static class State extends MobileActor.State {
        public State(String name, NameServer nameserver) { super(name, nameserver); }
        public State(String name, NameServer nameserver, int stage_id) { super(name, nameserver, stage_id); }

        public State(String name, NameServer nameserver, String host, int port) { super(name, nameserver, host, port); }

        public State(String name, NameServer nameserver, String host, int port, int stage_id) { super(name, nameserver, host, port, stage_id); }

        public void migrate(String host, int port) {
            if (! (host.equals(TransportService.getHost()) && port == TransportService.getPort()) ) {
                synchronized (MobileActorRegistry.getStateLock(this.hashCode())) {
                    MobileActorRegistry.updateStateEntry(this.hashCode(), TransportService.getSocket(host, port));
                }
                TransportService.migrateActor(host, port, this);
            }
        }

        String nodeId;
        ChordKey nodeKey;
        ChordNode predecessor;
        ChordNode successor;
        FingerTable fingerTable;
        boolean success = false;
        StandardOutput origin_output;
        StandardOutput local_output;
        Hashtable local;


        public Object invokeMessage(int messageId, Object[] arguments) throws RemoteMessageException, TokenPassException, MessageHandlerNotFoundException {
            switch(messageId) {
                case 0: return getPort();
                case 1: return getHost();
                case 2: return getName();
                case 3: return hashCode();
                case 4: return getLastKnownPort();
                case 5: return getLastKnownHost();
                case 6: migrate( (String)arguments[0], (Integer)arguments[1] ); return null;
                case 7: return getNameServer();
                case 8: go(); return null;
                case 9: create(); return null;
                case 10: stabilize(); return null;
                case 11: stabilize( (ChordNode)arguments[0] ); return null;
                case 12: stabilize( (ChordKey)arguments[0], (ChordKey)arguments[1], (ChordKey)arguments[2], (ChordNode)arguments[3] ); return null;
                case 13: notifyPredecessor( (ChordNode)arguments[0] ); return null;
                case 14: notifyPredecessor( (ChordNode)arguments[0], (ChordKey)arguments[1], (ChordKey)arguments[2], (ChordKey)arguments[3] ); return null;
                case 15: fixFingers(); return null;
                case 16: joinNode( (ChordNode)arguments[0] ); return null;
                case 17: joinSuccessor( (ChordNode)arguments[0] ); return null;
                case 18: return findSuccessor( (String)arguments[0] );
                case 19: return findSuccessor( (ChordKey)arguments[0] );
                case 20: return findSuccessor( (ChordKey)arguments[0], (ChordKey)arguments[1], (ChordKey)arguments[2] );
                case 21: return findSuccessor( (ChordKey)arguments[0], (ChordNode)arguments[1] );
                case 22: setSuccessorNode( (Finger)arguments[0], (ChordNode)arguments[1] ); return null;
                case 23: return closestPrecedingNode( (ChordKey)arguments[0] );
                case 24: nextClosestNode( (ChordKey)arguments[0], (ChordKey)arguments[1], (ChordKey)arguments[2] ); return null;
                case 25: return toString();
                case 26: printFingerTable(); return null;
                case 27: continuePrint( (Finger)arguments[0] ); return null;
                case 28: continueToPrint( (ChordKey)arguments[0], (ChordNode)arguments[1] ); return null;
                case 29: return getNodeId();
                case 30: setNodeId( (String)arguments[0] ); return null;
                case 31: return getNodeKey();
                case 32: setNodeKey( (ChordKey)arguments[0] ); return null;
                case 33: return getPredecessor();
                case 34: setPredecessor( (ChordNode)arguments[0] ); return null;
                case 35: return getSuccessor();
                case 36: setSuccessor( (ChordNode)arguments[0] ); return null;
                case 37: return getFingerTable();
                case 38: setFingerTable( (FingerTable)arguments[0] ); return null;
                default: throw new MessageHandlerNotFoundException(messageId, arguments);
            }
        }

        public void invokeConstructor(int messageId, Object[] arguments) throws RemoteMessageException, ConstructorNotFoundException {
            switch(messageId) {
                case 0: construct( (StandardOutput)arguments[0] ); return;
                default: throw new ConstructorNotFoundException(messageId, arguments);
            }
        }

        public void construct(StandardOutput out) {
            this.origin_output = out;
            this.local_output = StandardOutput.construct(0, null);
            this.nodeKey = new ChordKey( this.getName() );
            StageService.sendMessage(out, 12 /*println*/, new Object[]{this.getName() + "...." + this.nodeKey});
            this.nodeId = nodeKey.toString();
            this.fingerTable = FingerTable.construct(0, new Object[]{((DHashtable.ChordNode)this.getStage().message.target)});
            this.local = new Hashtable(  );
            StageService.sendMessage(((DHashtable.ChordNode)this.getStage().message.target), 9 /*create*/, null);
        }



        public void go() {
            ContinuationDirector continuation_token = StageService.sendContinuationMessage(StandardOutput.construct(0, null), 12 /*println*/, new Object[]{"Hello from: " + this.getName() + "!"});
            StageService.sendMessage(origin_output, 12 /*println*/, new Object[]{"Remote hello from: " + this.getName() + "!"}, continuation_token);
        }

        public void create() {
            predecessor = null;
            successor = ((DHashtable.ChordNode)this.getStage().message.target);
        }

        public void stabilize() {
            System.out.println(" \n\n");
            System.out.println("Stabilizing " + this.getName());
            TokenDirector node = StageService.sendTokenMessage(successor, 33 /*getPredecessor*/, null);
            StageService.sendMessage(((DHashtable.ChordNode)this.getStage().message.target), 11 /*stabilize*/, new Object[]{node}, new int[]{0});
        }

        public void stabilize(ChordNode node) throws TokenPassException {
            if (node != null) {
                System.out.println("Succ Pred is + " + node.getName());
                TokenDirector key = StageService.sendTokenMessage(node, 31 /*getNodeKey*/, null);
                TokenDirector selfKey = StageService.sendTokenMessage(((DHashtable.ChordNode)this.getStage().message.target), 31 /*getNodeKey*/, null);
                TokenDirector successorKey = StageService.sendTokenMessage(successor, 31 /*getNodeKey*/, null);
                StageService.sendPassMessage(((DHashtable.ChordNode)this.getStage().message.target), 12 /*stabilize*/, new Object[]{key, selfKey, successorKey, node}, new int[]{0, 1, 2}, this.getStage().message.continuationDirector);
                throw new TokenPassException();
            }
            
            StageService.sendMessage(successor, 13 /*notifyPredecessor*/, new Object[]{((DHashtable.ChordNode)this.getStage().message.target)});
        }

        public void stabilize(ChordKey key, ChordKey selfKey, ChordKey successorKey, ChordNode node) {
            if ((((DHashtable.ChordNode)this.getStage().message.target) == successor) || key.isBetween(selfKey, successorKey)) {
                successor = node;
            }
            
        }

        public void notifyPredecessor(ChordNode node) {
            if (predecessor != null) {
                System.out.println("Predecessor is + " + node.getName());
                TokenDirector predecessorKey = StageService.sendTokenMessage(predecessor, 31 /*getNodeKey*/, null);
                TokenDirector key = StageService.sendTokenMessage(node, 31 /*getNodeKey*/, null);
                TokenDirector selfKey = StageService.sendTokenMessage(((DHashtable.ChordNode)this.getStage().message.target), 31 /*getNodeKey*/, null);
                StageService.sendMessage(((DHashtable.ChordNode)this.getStage().message.target), 14 /*notifyPredecessor*/, new Object[]{node, key, selfKey, predecessorKey}, new int[]{1, 2, 3});
            }
            else {
                predecessor = node;
            }
        }

        public void notifyPredecessor(ChordNode node, ChordKey key, ChordKey selfKey, ChordKey predecessorKey) {
            if (key.isBetween(predecessorKey, selfKey)) {
                System.out.println("Pred Node is + " + node.getName());
                predecessor = node;
            }
            
        }

        public void fixFingers() {
            for (int i = 0; i < 32; i++) {
                TokenDirector finger = StageService.sendTokenMessage(fingerTable, 4 /*getFinger*/, new Object[]{i});
                TokenDirector key = StageService.sendTokenMessage(null, 2 /*getStart*/, null, finger);
                TokenDirector successorNode = StageService.sendTokenMessage(((DHashtable.ChordNode)this.getStage().message.target), 19 /*findSuccessor*/, new Object[]{key}, new int[]{0});
                StageService.sendMessage(((DHashtable.ChordNode)this.getStage().message.target), 22 /*setSuccessorNode*/, new Object[]{finger, successorNode}, new int[]{0, 1});
            }

        }

        public void joinNode(ChordNode rootNode) throws TokenPassException {
            predecessor = null;
            ChordKey key = new ChordKey( this.getName() );
            TokenDirector successor = StageService.sendTokenMessage(rootNode, 19 /*findSuccessor*/, new Object[]{(ChordKey)DeepCopy.deepCopy( key )});
            ContinuationDirector continuation_token = StageService.sendContinuationMessage(((DHashtable.ChordNode)this.getStage().message.target), 17 /*joinSuccessor*/, new Object[]{successor}, new int[]{0});
            continuation_token = StageService.sendContinuationMessage(local_output, 12 /*println*/, new Object[]{"node is  " + this.getName()}, continuation_token);
            class ExpressionDirector1 extends Actor {
                public ExpressionDirector1(int stage_id) { super(stage_id); }
                public void invokeConstructor(int id, Object[] arguments) {}
                public Object invokeMessage(int messageId, Object[] arguments) {
                    return "Succesor  " + (ChordNode)arguments[0];
                }
            }
            continuation_token = StageService.sendContinuationMessage(local_output, 12 /*println*/, new Object[]{StageService.sendImplicitTokenMessage(new ExpressionDirector1(this.getStageId()), 0, new Object[]{successor}, new int[]{0})}, new int[]{0}, continuation_token);
            StageService.sendPassMessage(local_output, 12 /*println*/, new Object[]{"Predecesor  " + predecessor}, continuation_token, this.getStage().message.continuationDirector);
            throw new TokenPassException();
        }

        public void joinSuccessor(ChordNode successor) {
            this.successor = successor;
        }

        public ChordNode findSuccessor(String nodeName) throws TokenPassException {
            ChordKey key = new ChordKey( nodeName );
            System.out.println("ChordKey found is " + key);
            StageService.sendPassMessage(((DHashtable.ChordNode)this.getStage().message.target), 19 /*findSuccessor*/, new Object[]{(ChordKey)DeepCopy.deepCopy( key )}, this.getStage().message.continuationDirector);
            throw new TokenPassException();
        }

        public ChordNode findSuccessor(ChordKey key) throws TokenPassException {
            if (((DHashtable.ChordNode)this.getStage().message.target) == successor) {
                return ((DHashtable.ChordNode)this.getStage().message.target);
            }
            
            System.out.println("attempt for successor");
            TokenDirector thisKey = StageService.sendTokenMessage(((DHashtable.ChordNode)this.getStage().message.target), 31 /*getNodeKey*/, null);
            TokenDirector successorKey = StageService.sendTokenMessage(successor, 31 /*getNodeKey*/, null);
            StageService.sendPassMessage(((DHashtable.ChordNode)this.getStage().message.target), 20 /*findSuccessor*/, new Object[]{(ChordKey)DeepCopy.deepCopy( key ), thisKey, successorKey}, new int[]{1, 2}, this.getStage().message.continuationDirector);
            throw new TokenPassException();
        }

        public ChordNode findSuccessor(ChordKey key, ChordKey thisKey, ChordKey successorKey) throws TokenPassException {
            if (key.isBetween(thisKey, successorKey) || key.compareTo(successorKey) == 0) {
                System.out.println("Successor is " + successor);
                return successor;
            }
            else {
                TokenDirector node = StageService.sendTokenMessage(((DHashtable.ChordNode)this.getStage().message.target), 23 /*closestPrecedingNode*/, new Object[]{(ChordKey)DeepCopy.deepCopy( key )});
                StageService.sendPassMessage(((DHashtable.ChordNode)this.getStage().message.target), 21 /*findSuccessor*/, new Object[]{(ChordKey)DeepCopy.deepCopy( key ), node}, new int[]{1}, this.getStage().message.continuationDirector);
                throw new TokenPassException();
            }

        }

        public ChordNode findSuccessor(ChordKey key, ChordNode node) throws TokenPassException {
            if (node == ((DHashtable.ChordNode)this.getStage().message.target) && successor != null) {
                StageService.sendPassMessage(successor, 19 /*findSuccessor*/, new Object[]{(ChordKey)DeepCopy.deepCopy( key )}, this.getStage().message.continuationDirector);
                throw new TokenPassException();
            }
            
            StageService.sendPassMessage(node, 19 /*findSuccessor*/, new Object[]{(ChordKey)DeepCopy.deepCopy( key )}, this.getStage().message.continuationDirector);
            throw new TokenPassException();
        }

        public void setSuccessorNode(Finger finger, ChordNode successorNode) {
            StageService.sendMessage(finger, 5 /*setNode*/, new Object[]{successorNode});
        }

        public ChordNode closestPrecedingNode(ChordKey key) throws TokenPassException {
            for (int i = 31; i >= 0; i--) {
                TokenDirector finger = StageService.sendTokenMessage(fingerTable, 4 /*getFinger*/, new Object[]{i});
                TokenDirector node = StageService.sendTokenMessage(null, 4 /*getNode*/, null, finger);
                TokenDirector fingerKey = StageService.sendTokenMessage(null, 31 /*getNodeKey*/, null, node);
                TokenDirector nodeKey = StageService.sendTokenMessage(((DHashtable.ChordNode)this.getStage().message.target), 31 /*getNodeKey*/, null);
                StageService.sendMessage(((DHashtable.ChordNode)this.getStage().message.target), 24 /*nextClosestNode*/, new Object[]{(ChordKey)DeepCopy.deepCopy( key ), nodeKey, fingerKey}, new int[]{1, 2});
                if (success == true) {
                    StageService.passToken(node, this.getStage().message.continuationDirector);
                    throw new TokenPassException();
                } 
            }

            return ((DHashtable.ChordNode)this.getStage().message.target);
        }

        public void nextClosestNode(ChordKey key, ChordKey nodeKey, ChordKey fingerKey) {
            if (fingerKey.isBetween(nodeKey, key)) {
                this.success = true;
            }
            
        }

        public String toString() {
            StringBuilder sb = new StringBuilder(  );
            sb.append("ChordNode");
            sb.append("ID=" + nodeId);
            sb.append(",KEY=" + nodeKey);
            return sb.toString();
        }

        public void printFingerTable() {
            System.out.println("=======================================================");
            System.out.println("FingerTable: " + ((DHashtable.ChordNode)this.getStage().message.target));
            System.out.println("-------------------------------------------------------");
            System.out.println("Predecessor: " + predecessor);
            System.out.println("Successor: " + successor);
            System.out.println("-------------------------------------------------------");
        }

        public void continuePrint(Finger finger) {
            TokenDirector start = StageService.sendTokenMessage(finger, 2 /*getStart*/, null);
            TokenDirector node = StageService.sendTokenMessage(finger, 4 /*getNode*/, null);
            StageService.sendMessage(((DHashtable.ChordNode)this.getStage().message.target), 28 /*continueToPrint*/, new Object[]{start, node}, new int[]{0, 1});
        }

        public void continueToPrint(ChordKey start, ChordNode node) {
            System.out.println(start + "\t" + node.getName());
            System.out.println("=======================================================");
        }

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public ChordKey getNodeKey() {
            return (ChordKey)DeepCopy.deepCopy( nodeKey );
        }

        public void setNodeKey(ChordKey nodeKey) {
            this.nodeKey = nodeKey;
        }

        public ChordNode getPredecessor() {
            return predecessor;
        }

        public void setPredecessor(ChordNode predecessor) {
            this.predecessor = predecessor;
        }

        public ChordNode getSuccessor() {
            return successor;
        }

        public void setSuccessor(ChordNode successor) {
            this.successor = successor;
        }

        public FingerTable getFingerTable() {
            return fingerTable;
        }

        public void setFingerTable(FingerTable fingerTable) {
            this.fingerTable = fingerTable;
        }


    }
}
